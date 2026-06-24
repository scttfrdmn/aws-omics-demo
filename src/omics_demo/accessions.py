"""
accessions.py  --  curated 1000 Genomes Project low-coverage WGS sample list.

~40 1000 Genomes Project phase 3 samples, balanced across three super-
populations (AFR / EUR / EAS).  Each sample's pre-aligned low-coverage BAM is
read DIRECTLY from the Open Data bucket:
  s3://1000genomes/phase3/data/<id>/alignment/<id>.mapped.ILLUMINA.bwa.<POP>.low_coverage.<date>.bam
(public, --no-sign-request, in us-east-1 — same region, $0 egress.)

The BAM paths below follow the 1000 Genomes phase3 low-coverage naming
convention:
  <sample_id>.mapped.ILLUMINA.bwa.<population>.low_coverage.<YYYYMMDD>.bam
The <date> stamp varies per sample (the phase3 alignment was released in
batches), so the filenames here use the 20120522 release stamp that covers the
bulk of the phase3 low-coverage set.  THESE PATHS MUST BE VALIDATED AGAINST A
LIVE BUCKET LISTING before a run, e.g.:
  aws s3 ls s3://1000genomes/phase3/data/NA12878/alignment/ --no-sign-request
and the date stamp corrected per sample if it differs.  Sample IDs and their
population/super-population assignments are stable (1000G phase3 panel).
"""

from __future__ import annotations

# Date stamp on the phase3 low-coverage BAM filenames. Most of the phase3
# low-coverage alignments carry the 20120522 release stamp; some samples differ
# and MUST be checked against a live listing (see module docstring).
_PHASE3_DATE = "20120522"


def _bam_path(sample_id: str, population: str) -> str:
    """Construct the 1000G phase3 low-coverage BAM S3 URI for a sample.

    Pattern (per the phase3 alignment release convention):
      s3://1000genomes/phase3/data/<id>/alignment/
        <id>.mapped.ILLUMINA.bwa.<POP>.low_coverage.<date>.bam
    """
    return (
        f"s3://1000genomes/phase3/data/{sample_id}/alignment/"
        f"{sample_id}.mapped.ILLUMINA.bwa.{population}.low_coverage."
        f"{_PHASE3_DATE}.bam"
    )


# (sample_id, population, super_population, bam_path)
# Populations: CEU/GBR/FIN/TSI=EUR, YRI/LWK/GWD/ESN=AFR, CHB/JPT/CHS/CDX=EAS.
GENOMES_1000_SAMPLES: list[tuple[str, str, str, str]] = [
    (sid, pop, spop, _bam_path(sid, pop))
    for sid, pop, spop in [
        # --- EUR (European) ---
        ("NA12878", "CEU", "EUR"),
        ("NA12889", "CEU", "EUR"),
        ("NA12890", "CEU", "EUR"),
        ("NA12891", "CEU", "EUR"),
        ("NA12892", "CEU", "EUR"),
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
        ("HG03053", "ESN", "AFR"),
        # --- EAS (East Asian) ---
        ("NA18525", "CHB", "EAS"),
        ("NA18526", "CHB", "EAS"),
        ("NA18527", "CHB", "EAS"),
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
]


def select_balanced(samples_per_group: int) -> list[tuple[str, str, str, str]]:
    """Return the first `samples_per_group` samples from each super-population.

    Balanced selector (analog of the old SAMPLES_PER_SITE selector): keeps the
    AFR/EUR/EAS groups equal-sized so population-differentiation QC isn't
    confounded by uneven group sizes. Preserves source order within each group.
    """
    by_group: dict[str, list[tuple[str, str, str, str]]] = {}
    for s in GENOMES_1000_SAMPLES:
        by_group.setdefault(s[2], []).append(s)
    selected: list[tuple[str, str, str, str]] = []
    for group in ("AFR", "EUR", "EAS"):
        selected.extend(by_group.get(group, [])[:samples_per_group])
    return selected
