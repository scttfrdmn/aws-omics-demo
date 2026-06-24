# Results — NOT YET RUN (what the study will measure)

> ⚠️ **No measurements exist yet.** This repo's pipeline has never been run. The
> harness, fairness protocol, and analysis code are in place; the tables below are
> **templates describing what a run will produce**, not data. Every value is shown
> as `—`. Do not cite anything here as a result until a real run fills it in.
>
> The shape of this study is ported from
> [aws-microbiome-demo](https://github.com/scttfrdmn/aws-microbiome-demo), which IS
> measured. The *science here is different* (germline variant calling on 1000
> Genomes vs. metagenomic profiling on HMP), so its numbers do **not** transfer.

The plan: run the **full from-scratch lifecycle** (provision → stage → run →
teardown) at **N** samples (balanced across 3 super-populations: AFR / EUR / EAS),
on a shared S3-backed FSx for Lustre filesystem holding the human reference genome,
native on each architecture, and report per-stage + end-to-end time / data / cost.

- Raw running log (to be created): `../benchmark/results/lifecycle/MEASUREMENTS.md`
- Per-leg JSON (to be created): `arm64-n<N>-fsx.json`, `x86-n<N>-fsx.json`, `variant_qc.json`
- How it will be measured: [methodology.md](methodology.md) · why the design: [decisions/](decisions/)

## End-to-end lifecycle (template)

| phase | arm64 time | x86 time | data moved | arm64 $ | x86 $ |
|-------|-----------:|---------:|-----------:|--------:|------:|
| stage (reference from `s3://1000genomes/technical/reference`) | — | — | — | — | — |
| provision (FSx + scoped DRA) | — | — | — | — | — |
| run (N samples, CALL_VARIANTS → MERGE_VCFS → VCF_STATS) | — | — | — | — | — |
| **total (1 run)** | — | — | — | — | — |

Will be split into **one-time** (stage + provision, amortized over R runs) vs
**per-run** (the run @ a given fan-out N), with N as the tunable knob — same
amortization treatment as the source study.

## Per-stage timing (template)

Will be billed by **EC2 instance lifetime** parsed from the head `nextflow.log`
(not trace `realtime`, which is wrapper-local on the spawn executor — see
[decisions/0002](decisions/0002-timing-instance-lifetime-not-trace.md)). Matched
instance pairs (c7g↔c7i, r7g↔r7i), same samples, same FSx-mounted reference.

| stage | tool | instance (arm64/x86) | arm64 median | x86 median | ratio |
|-------|------|----------------------|-------------:|-----------:|------:|
| CALL_VARIANTS (per-sample, the fan-out) | `bcftools mpileup -r 20 \| call -mv` | c7g/c7i.2xlarge | — | — | — |
| MERGE_VCFS (cohort fan-in) | `bcftools merge` | r7g/r7i.2xlarge | — | — | — |
| VCF_STATS (cohort QC) | `bcftools stats` | c7g/c7i.large | — | — | — |

## The Graviton price/performance question (the hypothesis to test)

The expectation, to be confirmed or refuted by measurement: **runtime roughly at
parity** per-stage between matched arm64/x86 pairs, with **arm64 cheaper** because
Graviton is ~19% lower $/hr at matched vCPU/RAM. Whether variant calling (an
I/O- and mpileup-bound workload, unlike the CPU-bound classifiers in the source
study) holds that pattern is exactly what this benchmark exists to find out — no
projecting from the metagenomics result.

## Scaling validation (the reason for FSx)

To be confirmed: the shared reference genome (~3 GB) on **one** FSx filesystem,
read concurrently by all N `CALL_VARIANTS` tasks, with no per-volume FSR credit
limit (the cliff that pushed the source study off EBS+FSR — see
[decisions/0001](decisions/0001-db-delivery-ami-ebs-fsx.md)). The reference should
never copy per task; each task symlinks into the shared mount.

## Variant-QC validation (the science check — planned)

Does the pipeline produce biologically sensible germline calls? The planned checks
(see [decisions/0004](decisions/0004-variant-qc-titv-and-population-differentiation.md)):

- **Ti/Tv ratio** — transition/transversion ratio, expected ~2.0–2.1 genome-wide
  for human WGS; a value far off suggests false-positive contamination. *(template)*
- **Variant counts** — total SNPs / indels on chr20 across the cohort. *(template)*
- **Population differentiation** — per-super-population allele frequencies should
  cluster by group (within-group allele-frequency distance < between-group), the
  variant-calling analog of body-site community separation. *(template)*

| metric | expected | measured |
|--------|----------|---------:|
| Ti/Tv ratio | ~2.0–2.1 | — |
| total variants (chr20) | — | — |
| within < between (super-pop differentiation) | within < between | — |

When the run happens, this page and the blog's results section get rewritten from
the actual `variant_qc.json` + lifecycle JSONs — nothing here is filled in by hand.
