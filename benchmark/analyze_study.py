#!/usr/bin/env python3
"""
analyze_study.py — the real-science deliverable for the x86-vs-arm64 1000 Genomes
variant-calling study.

Two independent things, from the artifacts of one balanced run per arch
(N per super-population × {AFR, EUR, EAS}):

  A. PER-STAGE ARCH BENCHMARK (timing + cost), with proper statistics.
     Source: the head's .nextflow.log (results/<job>/nextflow.head.log), NOT the
     Nextflow trace. On the spawn executor the trace realtime/duration columns are
     wrapper-local (sub-second); the AUTHORITATIVE per-task wall-clock is nf-spawn's
     lifecycle log — "Submitting task '<name>' to spawn instance 'nf-X'" and
     "Task '<name>' completed (exit C) on instance 'nf-X'", both timestamped. The
     submit→complete delta ≈ EC2 billed lifetime (boot + stage-in + compute +
     stage-out + terminate), which is what actually costs money. Per stage we
     report median + IQR + min–max, the arm64/x86 ratio, and a Mann-Whitney U
     test (nonparametric — no normality assumption) so "arm64 faster" is a claim,
     not a vibe. Cost = sum(per-task hours) × that stage's on-demand $/hr.

  B. VARIANT-CALLING QC + ARCH VALIDATION (the science).
     Source: bcftools stats (cohort QC) + the per-sample VCFs.
       - Ti/Tv ratio + SNP/indel counts (from bcftools stats / stats.json) —
         validate against the human-WGS expected window (~2.0–2.1).
       - population differentiation: per-super-population allele frequencies;
         samples should cluster by super-population (within-population
         allele-frequency distance < between-population). The analog of
         within<between Bray–Curtis beta diversity.
       - arch concordance: do arm64 and x86 produce the SAME calls per sample?
         (a correctness check — native-vs-native must agree)

Usage:
  python benchmark/analyze_study.py \
      --arm64-log benchmark/results/arm64/nextflow.head.log \
      --x86-log   benchmark/results/x86/nextflow.head.log \
      --arm64-vcf benchmark/results/arm64/variants \
      --x86-vcf   benchmark/results/x86/variants \
      --arm64-stats benchmark/results/arm64/stats.txt \
      --x86-stats   benchmark/results/x86/stats.txt \
      --json benchmark/results/study.json

Pure stdlib + a small Mann-Whitney implementation (no scipy dependency); if scipy
is present it's used for the U-test p-value, else a normal approximation is used
and labeled as such. VCFs are read with stdlib gzip — no pysam/cyvcf2 dependency.
"""

from __future__ import annotations

import argparse
import contextlib
import gzip
import json
import math
import re
import statistics
from collections import defaultdict
from datetime import datetime

# ── on-demand $/hr (us-east-1), matched arch pairs ──────────────────────────
PRICE = {
    "c7g.large": 0.0725,
    "c7g.2xlarge": 0.29,
    "r7g.2xlarge": 0.4288,
    "c7i.large": 0.0850,
    "c7i.2xlarge": 0.3400,
    "r7i.2xlarge": 0.5292,
}

# map a pipeline process name fragment → a stable stage label
STAGE_KEYS = [
    ("CALL_VARIANTS", "call_variants"),
    ("MERGE_VCFS", "merge_vcfs"),
    ("VCF_STATS", "vcf_stats"),
]

# Expected human-WGS Ti/Tv window — a calling-quality sanity check, not a target.
# ~2.0–2.1 genome-wide for WGS; a region-restricted call (chr20) sits in the same
# ballpark. Values far outside this flag a calling problem rather than biology.
TITV_EXPECTED = (1.9, 2.3)

_LOG_RE_SUBMIT = re.compile(
    r"^(\w{3}-\d{2} [\d:.]+).*Submitting task '([^']+)' to spawn instance '([^']+)' \(([^ ]+) in"
)
_LOG_RE_DONE = re.compile(
    r"^(\w{3}-\d{2} [\d:.]+).*Task '([^']+)' completed \(exit (\d+)\) on instance '([^']+)'"
)
_TS_FMT = "%b-%d %H:%M:%S.%f"


def _stage_of(task_name: str) -> str | None:
    for frag, label in STAGE_KEYS:
        if frag in task_name:
            return label
    return None


def parse_head_log(path: str) -> dict[str, list[dict]]:
    """Return {stage: [{task, instance, type, seconds, exit}, ...]} from a head log.

    Per-task wall-clock = complete_ts - submit_ts (≈ EC2 billed lifetime).
    """
    submits: dict[str, tuple[datetime, str, str]] = {}
    done: dict[str, tuple[datetime, int]] = {}
    with open(path) as f:
        for line in f:
            m = _LOG_RE_SUBMIT.search(line)
            if m:
                ts, name, inst, itype = m.groups()
                submits[name] = (datetime.strptime(ts, _TS_FMT), inst, itype)
                continue
            m = _LOG_RE_DONE.search(line)
            if m:
                ts, name, exit_code, _inst = m.groups()
                done[name] = (datetime.strptime(ts, _TS_FMT), int(exit_code))

    stages: dict[str, list[dict]] = defaultdict(list)
    for name, (sub_ts, inst, itype) in submits.items():
        if name not in done:
            continue
        comp_ts, exit_code = done[name]
        stage = _stage_of(name)
        if not stage:
            continue
        stages[stage].append(
            {
                "task": name,
                "instance": inst,
                "type": itype,
                "seconds": (comp_ts - sub_ts).total_seconds(),
                "exit": exit_code,
            }
        )
    return stages


# ── statistics (stdlib; scipy optional for exact U p-value) ─────────────────
def mann_whitney_u(a: list[float], b: list[float]) -> tuple[float, float, str]:
    """Return (U, p_two_sided, method). Normal approx unless scipy is available."""
    try:
        from scipy.stats import mannwhitneyu  # type: ignore

        U, p = mannwhitneyu(a, b, alternative="two-sided")
        return float(U), float(p), "scipy.mannwhitneyu"
    except Exception:
        pass
    # Normal approximation with tie correction.
    na, nb = len(a), len(b)
    if na == 0 or nb == 0:
        return float("nan"), float("nan"), "empty"
    combined = sorted([(v, 0) for v in a] + [(v, 1) for v in b])
    ranks = [0.0] * len(combined)
    i = 0
    while i < len(combined):
        j = i
        while j + 1 < len(combined) and combined[j + 1][0] == combined[i][0]:
            j += 1
        avg_rank = (i + j) / 2 + 1
        for k in range(i, j + 1):
            ranks[k] = avg_rank
        i = j + 1
    Ra = sum(r for r, (_, g) in zip(ranks, combined, strict=True) if g == 0)
    Ua = Ra - na * (na + 1) / 2
    Ub = na * nb - Ua
    U = min(Ua, Ub)
    mu = na * nb / 2
    sigma = math.sqrt(na * nb * (na + nb + 1) / 12)
    if sigma == 0:
        return U, 1.0, "normal-approx"
    z = (U - mu) / sigma
    p = 2 * (1 - 0.5 * (1 + math.erf(abs(z) / math.sqrt(2))))
    return U, p, "normal-approx"


def summarize_stage(tasks: list[dict]) -> dict:
    secs = sorted(t["seconds"] for t in tasks)
    itype = tasks[0]["type"] if tasks else "?"
    price = PRICE.get(itype, 0.0)
    cost = sum(t["seconds"] for t in tasks) / 3600 * price
    q = statistics.quantiles(secs, n=4) if len(secs) >= 2 else [secs[0]] * 3
    return {
        "n": len(secs),
        "type": itype,
        "price_per_hr": price,
        "median_s": round(statistics.median(secs), 1),
        "iqr_s": [round(q[0], 1), round(q[2], 1)],
        "min_s": round(min(secs), 1),
        "max_s": round(max(secs), 1),
        "cost_usd": round(cost, 4),
        "failures": sum(1 for t in tasks if t["exit"] != 0),
    }


def timing_report(arm_log: str, x86_log: str) -> dict:
    arm = parse_head_log(arm_log)
    x86 = parse_head_log(x86_log)
    out = {}
    for _, stage in STAGE_KEYS:
        a, x = arm.get(stage, []), x86.get(stage, [])
        if not a and not x:
            continue
        entry = {
            "arm64": summarize_stage(a) if a else None,
            "x86": summarize_stage(x) if x else None,
        }
        if a and x:
            asecs = [t["seconds"] for t in a]
            xsecs = [t["seconds"] for t in x]
            U, p, method = mann_whitney_u(asecs, xsecs)
            ma, mx = statistics.median(asecs), statistics.median(xsecs)
            entry["ratio_arm_over_x86"] = round(ma / mx, 3) if mx else None
            entry["mannwhitney"] = {
                "U": round(U, 1),
                "p_two_sided": round(p, 4),
                "method": method,
                "significant_0.05": p < 0.05,
            }
            entry["verdict"] = "arm64 faster" if ma < mx else "x86 faster" if mx < ma else "tie"
        out[stage] = entry
    return out


# ════════════════════════════════════════════════════════════════════════════
# B. VARIANT-CALLING QC + ARCH VALIDATION (the science)
# ════════════════════════════════════════════════════════════════════════════
#
# Inputs are the run's variant outputs, downloaded from the run's results prefix:
#   <vcf_dir>/<sample_id>.vcf.gz   (per-sample VCF, CALL_VARIANTS output)
#   <stats>  bcftools stats text OR the VCF_STATS stats.json (cohort QC)
# Super-population is recovered from the sample_id via the demo's 1000G panel.


def _sample_super_pop_map() -> dict[str, str]:
    """sample_id → super_population, from the demo's 1000 Genomes sample list."""
    import os
    import sys

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
    from omics_demo.accessions import GENOMES_1000_SAMPLES  # type: ignore

    return {sid: spop for sid, _pop, spop, _bam in GENOMES_1000_SAMPLES}


# ── bcftools stats: Ti/Tv + SNP/indel counts (call-quality QC) ───────────────
def parse_bcftools_stats(path: str) -> dict[str, float]:
    """Parse a `bcftools stats` text file → {ts_tv, snps, indels, records}.

    The 'SN' (summary numbers) lines are tab-separated; the value is the last
    field, keyed on the trailing label. ts/tv prints on its own SN line. This is
    the same signal VCF_STATS distils into stats.json — accept either input via
    load_cohort_stats()."""
    out: dict[str, float] = {}
    keys = {
        "number of SNPs:": "snps",
        "number of indels:": "indels",
        "number of records:": "records",
        "ts/tv:": "ts_tv",
    }
    with open(path) as f:
        for line in f:
            for label, key in keys.items():
                if label in line:
                    with contextlib.suppress(ValueError):
                        out[key] = float(line.rstrip("\n").split("\t")[-1].strip())
    return out


def load_cohort_stats(path: str) -> dict[str, float]:
    """Load cohort QC from either a bcftools-stats text file or a VCF_STATS
    stats.json ({total_records, snps, indels, ti_tv_ratio})."""
    if path.endswith(".json"):
        with open(path) as f:
            d = json.load(f)
        return {
            "ts_tv": float(d.get("ti_tv_ratio", 0) or 0),
            "snps": float(d.get("snps", 0) or 0),
            "indels": float(d.get("indels", 0) or 0),
            "records": float(d.get("total_records", 0) or 0),
        }
    return parse_bcftools_stats(path)


def titv_report(stats: dict[str, float]) -> dict:
    """Wrap cohort stats with the expected-window validation."""
    titv = stats.get("ts_tv", 0.0)
    lo, hi = TITV_EXPECTED
    return {
        "ti_tv_ratio": round(titv, 4),
        "snps": int(stats.get("snps", 0)),
        "indels": int(stats.get("indels", 0)),
        "records": int(stats.get("records", 0)),
        "expected_window": [lo, hi],
        "in_expected_window": bool(lo <= titv <= hi),
    }


# ── per-sample VCF → allele frequencies → population differentiation ─────────
_GT_RE = re.compile(r"^([.\d]+)[/|]([.\d]+)")


def _open(path: str):
    return gzip.open(path, "rt") if path.endswith(".gz") else open(path)


def parse_vcf_dosages(path: str) -> dict[str, float]:
    """Per-sample VCF → {site_key: alt-allele dosage 0/1/2}.

    site_key = 'CHROM:POS:REF:ALT'. Dosage is the count of ALT alleles in this
    sample's diploid genotype (0, 1, or 2) — the per-sample allele-frequency
    contribution. Multi-sample VCFs are not expected here (CALL_VARIANTS emits one
    sample each), so we read the FORMAT/GT of the single sample column. Missing
    genotypes (./.) are skipped."""
    out: dict[str, float] = {}
    with _open(path) as f:
        for line in f:
            if line.startswith("#") or not line.strip():
                continue
            fields = line.rstrip("\n").split("\t")
            if len(fields) < 10:
                continue
            chrom, pos, _id, ref, alt = fields[0], fields[1], fields[2], fields[3], fields[4]
            if "," in alt:  # skip multiallelic for a clean biallelic dosage
                continue
            fmt = fields[8].split(":")
            sample = fields[9].split(":")
            if "GT" not in fmt:
                continue
            gt = sample[fmt.index("GT")]
            m = _GT_RE.match(gt)
            if not m:
                continue
            a1, a2 = m.group(1), m.group(2)
            if a1 == "." or a2 == ".":
                continue
            dosage = (1 if a1 != "0" else 0) + (1 if a2 != "0" else 0)
            out[f"{chrom}:{pos}:{ref}:{alt}"] = float(dosage)
    return out


def _group_allele_freqs(dosages_by_sample: dict[str, dict[str, float]]) -> dict[str, float]:
    """Aggregate per-sample alt-allele dosages into a group allele-frequency vector
    {site_key: alt freq in [0,1]}. Freq = Σ dosage / (2 × n samples carrying GT)."""
    site_sum: dict[str, float] = defaultdict(float)
    site_n: dict[str, int] = defaultdict(int)
    for dosages in dosages_by_sample.values():
        for site, d in dosages.items():
            site_sum[site] += d
            site_n[site] += 1
    return {site: site_sum[site] / (2 * site_n[site]) for site in site_sum if site_n[site]}


def _freq_distance(a: dict[str, float], b: dict[str, float]) -> float:
    """Mean absolute allele-frequency difference over the union of sites (sites
    absent in one group contribute that group's freq vs 0). This is the
    population-genetics analog of a community-distance metric: it measures how
    differently two groups' allele frequencies are distributed."""
    keys = set(a) | set(b)
    if not keys:
        return 0.0
    return sum(abs(a.get(k, 0.0) - b.get(k, 0.0)) for k in keys) / len(keys)


def load_vcfs(vcf_dir: str) -> dict[str, dict]:
    """Return {sample_id: {sample_id, super_population, dosages:{site:dosage}}}.

    Scans a downloaded variants dir for <sample_id>.vcf.gz files."""
    import glob
    import os

    spop_map = _sample_super_pop_map()
    samples: dict[str, dict] = {}
    for path in glob.glob(os.path.join(vcf_dir or "", "**", "*.vcf.gz"), recursive=True):
        base = os.path.basename(path)
        sid = base[: -len(".vcf.gz")]
        samples[sid] = {
            "sample_id": sid,
            "super_population": spop_map.get(sid, "unknown"),
            "dosages": parse_vcf_dosages(path),
        }
    return samples


def population_differentiation(samples: dict) -> dict:
    """Mean allele-frequency distance within vs between super-populations — do
    samples cluster by super-population? (within < between is the expected,
    validating signal; the analog of within<between Bray–Curtis beta diversity.)

    Per-super-population allele-frequency vectors are computed from the per-sample
    alt-allele dosages, then we compare:
      - within: distance between each group's allele freqs and itself across random
        sample splits → here, the spread among samples of the same group;
      - between: distance between distinct super-populations' group allele freqs.
    """
    by_group: dict[str, dict[str, dict[str, float]]] = defaultdict(dict)
    for sid, s in samples.items():
        if s.get("dosages"):
            by_group[s["super_population"]][sid] = s["dosages"]

    # Group-level allele-frequency vectors (the between comparison).
    group_freqs = {g: _group_allele_freqs(members) for g, members in by_group.items()}

    # within: each sample's own freq vector (dosage/2) vs its group's freq vector.
    within: list[float] = []
    for g, members in by_group.items():
        gf = group_freqs[g]
        for _sid, dosages in members.items():
            sample_freq = {k: v / 2.0 for k, v in dosages.items()}
            within.append(_freq_distance(sample_freq, gf))

    # between: each pair of distinct super-population group freq vectors.
    between: list[float] = []
    groups = [g for g in group_freqs if g != "unknown"]
    for i, gi in enumerate(groups):
        for gj in groups[i + 1 :]:
            between.append(_freq_distance(group_freqs[gi], group_freqs[gj]))

    return {
        "super_populations": sorted(groups),
        "within_pop_mean": round(statistics.mean(within), 4) if within else None,
        "between_pop_mean": round(statistics.mean(between), 4) if between else None,
        "n_within": len(within),
        "n_between": len(between),
        "differentiates_by_pop": (
            bool(within and between and statistics.mean(within) < statistics.mean(between))
        ),
    }


def arch_concordance(arm: dict, x86: dict) -> dict:
    """Do arm64 and x86 produce the SAME calls per sample? Correctness check —
    native-vs-native must agree. Reports per-sample variant-site Jaccard over the
    called sites."""
    shared = [s for s in arm if s in x86 and arm[s].get("dosages") and x86[s].get("dosages")]
    jac = []
    for s in shared:
        a, x = set(arm[s]["dosages"]), set(x86[s]["dosages"])
        if a or x:
            jac.append(len(a & x) / len(a | x))
    return {
        "n_shared_samples": len(shared),
        "site_jaccard_median": round(statistics.median(jac), 3) if jac else None,
        "concordant": bool(jac and statistics.median(jac) >= 0.95),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--arm64-log", required=True)
    ap.add_argument("--x86-log", required=True)
    # QC (optional): point at downloaded per-sample VCFs + cohort bcftools stats.
    ap.add_argument("--arm64-vcf", help="dir of arm64 per-sample <sample>.vcf.gz")
    ap.add_argument("--x86-vcf", help="dir of x86 per-sample <sample>.vcf.gz")
    ap.add_argument("--arm64-stats", help="arm64 bcftools stats .txt or VCF_STATS stats.json")
    ap.add_argument("--x86-stats", help="x86 bcftools stats .txt or VCF_STATS stats.json")
    ap.add_argument("--json")
    args = ap.parse_args()

    result = {}

    # ── A. timing ────────────────────────────────────────────────────────────
    timing = timing_report(args.arm64_log, args.x86_log)
    result["timing"] = timing

    print("=" * 78)
    print("PER-STAGE ARCH BENCHMARK — billed wall-clock from nf-spawn lifecycle log")
    print("=" * 78)
    print(f"{'stage':18} {'arm64 med':>10} {'x86 med':>10} {'ratio':>7} {'p':>8}  verdict")
    for stage, e in timing.items():
        a, x = e.get("arm64"), e.get("x86")
        am = f"{a['median_s']:.0f}s" if a else "-"
        xm = f"{x['median_s']:.0f}s" if x else "-"
        ratio = e.get("ratio_arm_over_x86", "-")
        p = e.get("mannwhitney", {}).get("p_two_sided", "-")
        v = e.get("verdict", "")
        sig = " *" if e.get("mannwhitney", {}).get("significant_0.05") else ""
        print(f"{stage:18} {am:>10} {xm:>10} {str(ratio):>7} {str(p):>8}  {v}{sig}")
    arm_cost = sum((e["arm64"] or {}).get("cost_usd", 0) for e in timing.values())
    x86_cost = sum((e["x86"] or {}).get("cost_usd", 0) for e in timing.values())
    result["cost"] = {"arm64": round(arm_cost, 4), "x86": round(x86_cost, 4)}
    print(
        f"\nTask compute cost (sum billed-time × $/hr): arm64 ${arm_cost:.3f}  x86 ${x86_cost:.3f}"
    )
    fails = sum(
        (e.get("arm64") or {}).get("failures", 0) + (e.get("x86") or {}).get("failures", 0)
        for e in timing.values()
    )
    print(f"Failed tasks across both legs: {fails}  (any nonzero invalidates the comparison)")
    print("\nNote: '*' = Mann-Whitney U significant at p<0.05. ratio<1 → arm64 faster.")

    # ── B. variant-calling QC (when VCF / stats outputs are provided) ──────────
    if args.arm64_vcf or args.x86_vcf or args.arm64_stats or args.x86_stats:
        qc = {"arm64": {}, "x86": {}, "concordance": {}}
        for leg, vcf_dir, stats_path in (
            ("arm64", args.arm64_vcf, args.arm64_stats),
            ("x86", args.x86_vcf, args.x86_stats),
        ):
            if stats_path:
                qc[leg]["titv"] = titv_report(load_cohort_stats(stats_path))
            if vcf_dir:
                samples = load_vcfs(vcf_dir)
                qc[leg]["_samples"] = samples
                qc[leg]["population"] = population_differentiation(samples)

        arm_s = qc["arm64"].pop("_samples", {})
        x86_s = qc["x86"].pop("_samples", {})
        if arm_s and x86_s:
            qc["concordance"] = arch_concordance(arm_s, x86_s)
        result["qc"] = qc

        print("\n" + "=" * 78)
        print("VARIANT-CALLING QC — Ti/Tv + population differentiation")
        print("=" * 78)
        for leg in ("arm64", "x86"):
            titv = qc[leg].get("titv")
            if titv:
                ok = "✓" if titv["in_expected_window"] else "⚠ OUT OF WINDOW"
                print(f"\n[{leg}] cohort QC")
                print(
                    f"  Ti/Tv = {titv['ti_tv_ratio']}  (expected {titv['expected_window']})  {ok}"
                )
                print(f"  SNPs={titv['snps']}  indels={titv['indels']}  records={titv['records']}")
            pop = qc[leg].get("population")
            if pop:
                sep = (
                    "✓ differentiates by super-pop"
                    if pop["differentiates_by_pop"]
                    else "⚠ no separation"
                )
                print(f"  super-pops: {', '.join(pop['super_populations'])}")
                print(
                    f"  allele-freq distance: within={pop['within_pop_mean']} "
                    f"between={pop['between_pop_mean']}  {sep}"
                )

        if qc["concordance"]:
            c = qc["concordance"]
            ok = "✓ concordant" if c["concordant"] else "⚠ DIVERGENT"
            print("\n" + "=" * 78)
            print("ARCH CONCORDANCE — arm64 vs x86 must agree (correctness check)")
            print("=" * 78)
            print(
                f"  n={c['n_shared_samples']} shared  "
                f"variant-site Jaccard={c['site_jaccard_median']}  {ok}"
            )

    if args.json:
        with open(args.json, "w") as f:
            json.dump(result, f, indent=2)
        print(f"\nWrote {args.json}")


if __name__ == "__main__":
    main()
