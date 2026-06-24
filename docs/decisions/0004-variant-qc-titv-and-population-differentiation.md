# 0004 — Variant-calling QC: Ti/Tv ratio and population differentiation

**Status:** accepted (planned validation; harness ready — NOT YET RUN)
**Date:** 2026-06-24

## Context

The science validation (`analyze_study.py`) asks: are the variant calls credible,
and do they recover real 1000 Genomes population structure? This record fixes the
**planned** validation approach — the metrics the harness computes and the values
we expect — *before* the study is run. No measurements exist yet; everything below
is the expected/target signal, not a claim of results.

The QC has two parts, mirroring alpha/beta diversity in the microbiome predecessor:
a per-cohort **quality** metric (Ti/Tv) and a per-group **structure** metric
(population differentiation).

## Metric 1 — Ti/Tv ratio (call quality)

The **transition/transversion ratio** is the standard human variant-QC metric.
`bcftools stats` emits it directly (the `ts/tv` summary line), and `VCF_STATS`
publishes it to `stats.json` alongside SNP/indel counts.

- **Expected:** ~**2.0–2.1** genome-wide for human WGS. A region-restricted call
  (here chromosome 20 for demo speed) sits in the same ballpark; coding-only subsets
  run higher (~3), random/false calls drag toward ~0.5.
- **How it validates:** a Ti/Tv far from the expected window flags a calling
  problem (bad reference, wrong contig naming, spurious calls) rather than real
  biology. It is the variant-calling analog of "does Shannon diversity land in a
  sane range" — a single cheap number that catches a broken pipeline.

`analyze_study.py` reports the cohort Ti/Tv with the SNP/indel split, and flags it
against the expected window.

## Metric 2 — population differentiation (structure)

Per-super-population **allele frequencies** computed from the per-sample VCFs (or
the merged cohort VCF), grouped by `super_population` (AFR / EUR / EAS). The
validating signal: samples cluster by super-population —
**within-population allele-frequency distance < between-population distance**.

- **Expected:** real 1000 Genomes super-populations differ in allele frequencies at
  many sites (the basis of population genetics), so the mean within-population
  allele-frequency distance should be **measurably below** the mean
  between-population distance.
- **How it validates:** this is the variant-calling analog of within<between
  Bray–Curtis beta diversity. If the within and between distances are
  indistinguishable, either the calls are noise or the cohort is too small/skewed
  to resolve structure — both worth knowing before trusting any downstream claim.

`analyze_study.py` computes per-group allele-frequency vectors, then the mean
within-group vs between-group distance, and reports whether `within < between`.

## Cautions carried forward from the predecessor

The microbiome study learned (the hard way) that a structure metric on genomic data
can produce a **false null** if you summarize a saturated/bimodal pairwise
distribution carelessly, or compare at the wrong granularity. Carried into variant
QC as design caution, not as observed bugs:

- **Inspect the distribution**, not just one separation number, before declaring
  "separates by population." Report the spread, not only a single mean.
- **Frequency, not presence/absence.** Two samples from different super-populations
  share most *sites*; the signal is in allele *frequency*, so the distance must be
  computed on frequencies, not on a shared-variant set that would saturate.
- A **small or unbalanced cohort** can wash out real structure; the balanced N/3
  per super-population (AFR/EUR/EAS) selector keeps group sizes equal so the
  comparison isn't confounded by sample count.

## Lesson (forward-looking)

Variant-calling QC needs both a **quality** check (Ti/Tv in the expected window)
and a **structure** check (population differentiation, within < between), and the
structure check is only trustworthy if you (a) compute it on allele *frequencies*,
(b) keep groups balanced, and (c) look at the distribution rather than a single
threshold. These are the same lessons the microbiome predecessor paid for on beta
diversity, applied up front here.
