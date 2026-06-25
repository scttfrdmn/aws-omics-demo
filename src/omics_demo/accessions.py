"""
accessions.py  --  curated 1000 Genomes Project low-coverage WGS sample list.

108 1000 Genomes Project phase 3 samples, balanced 36 per super-population
(AFR / EUR / EAS) — enough for the talk's "100 genomes" headline with headroom
for any that fail to resolve. Sourced from the authoritative panel
(s3://1000genomes/release/20130502/integrated_call_samples_v3.20130502.ALL.panel)
and each verified to have a chrom20 low-coverage BAM. Each sample's pre-aligned,
per-chromosome low-coverage BAM is read DIRECTLY from the Open Data bucket (public,
--no-sign-request, in us-east-1 — same region, $0 egress).

The pipeline only calls variants on **chromosome 20**, so we use each sample's
`chrom20` BAM (~300-800 MB) rather than the whole-genome `mapped` BAM (~15-40 GB)
— ~50x less data read per task, and exactly the slice the pipeline needs:
  s3://1000genomes/phase3/data/<id>/alignment/<id>.chrom20.ILLUMINA.bwa.<POP>.low_coverage.<date>.bam

The <date> stamp varies per sample (phase3 was released in batches), so paths are
NOT hardcoded — `resolve_bam_paths()` lists each sample's alignment dir at runtime
and picks the chrom20 low-coverage BAM, skipping any sample that lacks one. The
`bam_path` field in the static table below is left empty and filled in by the
resolver. Sample IDs and population/super-population assignments are stable
(1000G phase3 panel).
"""

from __future__ import annotations

import re
import subprocess

_ALIGN_PREFIX = "s3://1000genomes/phase3/data/{sid}/alignment/"
# A sample's chrom20 low-coverage BAM (NOT .bai/.bas/.cram), any date stamp.
_CHROM20_RE = re.compile(
    r"^(?P<f>[A-Z0-9]+\.chrom20\.ILLUMINA\.bwa\.[A-Z]+\.low_coverage\.\d+\.bam)$"
)


# (sample_id, population, super_population, bam_path)
# bam_path is resolved at runtime by resolve_bam_paths(); "" in the static table.
# Populations: CEU/GBR/FIN/TSI=EUR, YRI/LWK/GWD/ESN=AFR, CHB/JPT/CHS/CDX=EAS.
_PANEL: list[tuple[str, str, str]] = [
    # --- EUR (European) ---
    ("HG00096", "GBR", "EUR"),
    ("HG00097", "GBR", "EUR"),
    ("HG00099", "GBR", "EUR"),
    ("HG00100", "GBR", "EUR"),
    ("HG00101", "GBR", "EUR"),
    ("HG00102", "GBR", "EUR"),
    ("HG00103", "GBR", "EUR"),
    ("HG00105", "GBR", "EUR"),
    ("HG00106", "GBR", "EUR"),
    ("HG00107", "GBR", "EUR"),
    ("HG00108", "GBR", "EUR"),
    ("HG00109", "GBR", "EUR"),
    ("HG00110", "GBR", "EUR"),
    ("HG00111", "GBR", "EUR"),
    ("HG00112", "GBR", "EUR"),
    ("HG00113", "GBR", "EUR"),
    ("HG00114", "GBR", "EUR"),
    ("HG00115", "GBR", "EUR"),
    ("HG00116", "GBR", "EUR"),
    ("HG00117", "GBR", "EUR"),
    ("HG00118", "GBR", "EUR"),
    ("HG00119", "GBR", "EUR"),
    ("HG00120", "GBR", "EUR"),
    ("HG00121", "GBR", "EUR"),
    ("HG00122", "GBR", "EUR"),
    ("HG00123", "GBR", "EUR"),
    ("HG00125", "GBR", "EUR"),
    ("HG00126", "GBR", "EUR"),
    ("HG00127", "GBR", "EUR"),
    ("HG00128", "GBR", "EUR"),
    ("HG00129", "GBR", "EUR"),
    ("HG00130", "GBR", "EUR"),
    ("HG00131", "GBR", "EUR"),
    ("HG00132", "GBR", "EUR"),
    ("HG00133", "GBR", "EUR"),
    ("HG00136", "GBR", "EUR"),
    # --- AFR (African) ---
    ("HG01879", "ACB", "AFR"),
    ("HG01880", "ACB", "AFR"),
    ("HG01882", "ACB", "AFR"),
    ("HG01883", "ACB", "AFR"),
    ("HG01885", "ACB", "AFR"),
    ("HG01886", "ACB", "AFR"),
    ("HG01889", "ACB", "AFR"),
    ("HG01890", "ACB", "AFR"),
    ("HG01894", "ACB", "AFR"),
    ("HG01896", "ACB", "AFR"),
    ("HG01912", "ACB", "AFR"),
    ("HG01914", "ACB", "AFR"),
    ("HG01915", "ACB", "AFR"),
    ("HG01956", "ACB", "AFR"),
    ("HG01958", "ACB", "AFR"),
    ("HG01985", "ACB", "AFR"),
    ("HG01986", "ACB", "AFR"),
    ("HG01988", "ACB", "AFR"),
    ("HG01989", "ACB", "AFR"),
    ("HG01990", "ACB", "AFR"),
    ("HG02009", "ACB", "AFR"),
    ("HG02010", "ACB", "AFR"),
    ("HG02012", "ACB", "AFR"),
    ("HG02013", "ACB", "AFR"),
    ("HG02014", "ACB", "AFR"),
    ("HG02051", "ACB", "AFR"),
    ("HG02052", "ACB", "AFR"),
    ("HG02053", "ACB", "AFR"),
    ("HG02054", "ACB", "AFR"),
    ("HG02095", "ACB", "AFR"),
    ("HG02107", "ACB", "AFR"),
    ("HG02108", "ACB", "AFR"),
    ("HG02111", "ACB", "AFR"),
    ("HG02143", "ACB", "AFR"),
    ("HG02144", "ACB", "AFR"),
    ("HG02255", "ACB", "AFR"),
    # --- EAS (East Asian) ---
    ("HG00403", "CHS", "EAS"),
    ("HG00404", "CHS", "EAS"),
    ("HG00406", "CHS", "EAS"),
    ("HG00407", "CHS", "EAS"),
    ("HG00409", "CHS", "EAS"),
    ("HG00410", "CHS", "EAS"),
    ("HG00419", "CHS", "EAS"),
    ("HG00421", "CHS", "EAS"),
    ("HG00422", "CHS", "EAS"),
    ("HG00428", "CHS", "EAS"),
    ("HG00436", "CHS", "EAS"),
    ("HG00437", "CHS", "EAS"),
    ("HG00442", "CHS", "EAS"),
    ("HG00443", "CHS", "EAS"),
    ("HG00445", "CHS", "EAS"),
    ("HG00446", "CHS", "EAS"),
    ("HG00448", "CHS", "EAS"),
    ("HG00449", "CHS", "EAS"),
    ("HG00451", "CHS", "EAS"),
    ("HG00452", "CHS", "EAS"),
    ("HG00457", "CHS", "EAS"),
    ("HG00458", "CHS", "EAS"),
    ("HG00463", "CHS", "EAS"),
    ("HG00464", "CHS", "EAS"),
    ("HG00472", "CHS", "EAS"),
    ("HG00473", "CHS", "EAS"),
    ("HG00475", "CHS", "EAS"),
    ("HG00476", "CHS", "EAS"),
    ("HG00478", "CHS", "EAS"),
    ("HG00479", "CHS", "EAS"),
    ("HG00500", "CHS", "EAS"),
    ("HG00513", "CHS", "EAS"),
    ("HG00524", "CHS", "EAS"),
    ("HG00525", "CHS", "EAS"),
    ("HG00530", "CHS", "EAS"),
    ("HG00531", "CHS", "EAS"),
]

# Static list with empty bam_path (resolved at runtime). Kept for back-compat
# with callers that read counts/IDs without needing a live S3 listing.
GENOMES_1000_SAMPLES: list[tuple[str, str, str, str]] = [
    (sid, pop, spop, "") for sid, pop, spop in _PANEL
]


def _resolve_one(sample_id: str) -> str | None:
    """List a sample's alignment dir and return its chrom20 low-coverage BAM URI,
    or None if it has none. Read-only, --no-sign-request (public bucket)."""
    prefix = _ALIGN_PREFIX.format(sid=sample_id)
    try:
        out = subprocess.run(
            ["aws", "s3", "ls", prefix, "--no-sign-request"],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    for line in out.splitlines():
        name = line.split()[-1] if line.split() else ""
        if _CHROM20_RE.match(name):
            return prefix + name
    return None


def resolve_bam_paths(
    samples: list[tuple[str, str, str, str]],
) -> list[tuple[str, str, str, str]]:
    """Fill in each sample's chrom20 BAM path from a live S3 listing.

    Returns only samples whose chrom20 low-coverage BAM actually exists; samples
    without one are dropped (logged to stderr). This avoids hardcoding the
    per-sample date stamp, which varies across the phase3 release batches.
    """
    import sys

    resolved: list[tuple[str, str, str, str]] = []
    for sid, pop, spop, _ in samples:
        uri = _resolve_one(sid)
        if uri:
            resolved.append((sid, pop, spop, uri))
        else:
            print(f"  [accessions] no chrom20 BAM for {sid} — skipping", file=sys.stderr)
    return resolved


def select_balanced(
    samples_per_group: int, resolve: bool = True
) -> list[tuple[str, str, str, str]]:
    """Return up to `samples_per_group` samples from each super-population.

    Balanced selector: keeps the AFR/EUR/EAS groups equal-sized so
    population-differentiation QC isn't confounded by uneven group sizes. When
    `resolve` is True (default), BAM paths are resolved against a live S3 listing
    and any sample without a chrom20 BAM is dropped BEFORE the per-group cut, so
    each group still yields `samples_per_group` valid samples where available.
    """
    pool = resolve_bam_paths(GENOMES_1000_SAMPLES) if resolve else GENOMES_1000_SAMPLES
    by_group: dict[str, list[tuple[str, str, str, str]]] = {}
    for s in pool:
        by_group.setdefault(s[2], []).append(s)
    selected: list[tuple[str, str, str, str]] = []
    for group in ("AFR", "EUR", "EAS"):
        selected.extend(by_group.get(group, [])[:samples_per_group])
    return selected
