"""
accessions.py  --  curated 1000 Genomes Project low-coverage WGS sample list.

~40 1000 Genomes Project phase 3 samples, balanced across three super-
populations (AFR / EUR / EAS).  Each sample's pre-aligned, per-chromosome
low-coverage BAM is read DIRECTLY from the Open Data bucket (public,
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
    ("NA12878", "CEU", "EUR"),
    ("NA12889", "CEU", "EUR"),
    ("NA12890", "CEU", "EUR"),
    ("HG00096", "GBR", "EUR"),
    ("HG00097", "GBR", "EUR"),
    ("HG00099", "GBR", "EUR"),
    ("HG00100", "GBR", "EUR"),
    ("HG00171", "FIN", "EUR"),
    ("HG00173", "FIN", "EUR"),
    ("NA20502", "TSI", "EUR"),
    ("NA20503", "TSI", "EUR"),
    # --- AFR (African) ---
    ("NA19238", "YRI", "AFR"),
    ("NA19239", "YRI", "AFR"),
    ("NA19240", "YRI", "AFR"),
    ("NA19247", "YRI", "AFR"),
    ("NA19256", "YRI", "AFR"),
    ("NA19026", "LWK", "AFR"),
    ("NA19027", "LWK", "AFR"),
    ("NA19028", "LWK", "AFR"),
    ("HG02922", "GWD", "AFR"),
    ("HG02923", "GWD", "AFR"),
    ("HG02938", "GWD", "AFR"),
    ("HG03052", "ESN", "AFR"),
    # --- EAS (East Asian) ---
    ("NA18525", "CHB", "EAS"),
    ("NA18526", "CHB", "EAS"),
    ("NA18528", "CHB", "EAS"),
    ("NA18537", "CHB", "EAS"),
    ("NA18939", "JPT", "EAS"),
    ("NA18940", "JPT", "EAS"),
    ("NA18941", "JPT", "EAS"),
    ("HG00403", "CHS", "EAS"),
    ("HG00404", "CHS", "EAS"),
    ("HG00406", "CHS", "EAS"),
    ("HG01798", "CDX", "EAS"),
    ("HG01799", "CDX", "EAS"),
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
