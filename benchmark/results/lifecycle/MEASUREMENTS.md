# Measurements — 1000 Genomes variant calling, arm64 vs x86

Real, measured results. Germline variant calling (bcftools mpileup→call→merge→stats)
on 1000 Genomes low-coverage chr20 BAMs, one ephemeral EC2 instance per sample via
spawn/nf-spawn, shared human reference read off FSx Lustre. Account 942542972736,
us-east-1a. Validated 2026-06-25.

## Science — identical across architectures (correctness precondition)

| metric | N=3 | N=30 |
|--------|----:|-----:|
| total variants | 154,877 | 422,877 |
| SNPs | 136,483 | 373,041 |
| indels | 18,394 | 49,836 |
| Ti/Tv ratio | 2.21 | 1.97 |

**arm64 and x86 produce bit-identical variant calls** at both scales (same counts,
same Ti/Tv; VCF byte sizes differ only by the gzip header). Ti/Tv drifts 2.21→1.97
as the cohort grows from 3→30 samples — expected: more (rarer) variants from a
larger, more diverse panel pull the ratio toward the genome-wide ~2.0.

Cohort: balanced 10/10/10 across super-populations AFR / EUR / EAS (1000G phase3).

## Performance — per-sample bcftools wall-clock (median over N tasks)

| | arm64 (c7g.2xlarge) | x86 (c7i.2xlarge) |
|--|--------------------:|------------------:|
| N=30 median | 124.0 s | **102.3 s** |
| N=30 min–max | 92.1 – 171.8 s | 79.5 – 144.5 s |
| N=3 (3 samples) | 110–143 s | 117–162 s |

**At N=30, x86 ran bcftools ~17% faster** (median 102s vs 124s). The N=3 numbers
hinted the opposite (arm64 ahead) — that was small-N noise; the N=30 medians (30
tasks each) are the trustworthy signal. **For this variant-calling workload it's
roughly a wash**, with x86/c7i slightly ahead per-task at scale. The cost decision
is then $/throughput: Graviton's lower $/hr vs x86's faster runtime — close enough
that you'd benchmark your own cohort rather than assume a winner. (Contrast the
CPU-bound Kraken2 case in the microbiome demo, where Graviton won outright.)

⚠️ Single run per (arch, N). Per-stage medians over 30 tasks are robust; the cross-
arch ratio is one observation. The Ti/Tv + variant counts are deterministic.

## Scaling — 3 → 30, clean

| | wall-clock | peak concurrent CALL_VARIANTS | failed |
|--|-----------:|------------------------------:|-------:|
| N=3 | ~5–6 min | 3 | 0 |
| N=30 | ~8.3 min | **30** | 0 |

- **True 30-way fan-out** — one EC2 instance per sample, peak 30 concurrent, no
  throttle (truffle-derived queueSize=585 ≫ 30).
- **No new failure modes at scale.** 30 simultaneous bcftools container pulls from
  quay.io: no rate-limit. **30 concurrent readers on the one FSx filesystem: flat
  per-task time** (median held ~same as N=3) — no reader contention; the ~3 GB
  reference caches after first touch. This is the shared-reference-on-FSx thesis,
  confirmed at 30 readers.
- Near-flat wall-clock scaling (work parallelizes; the extra ~2-3 min is the longer
  task tail + bigger MERGE_VCFS).
- BAM download from s3://1000genomes (same region, $0 egress): median ~105 MB/s
  (arm64) / ~126 MB/s (x86) — network jitter, not architectural.

## Files
`{arm64,x86}-n30/` — stats.json, summary.json, nextflow.head.log, staging/ (30
per-sample timing JSONs). `{arm64,x86}-n3-smoke/` — the N=3 shake-out results.

## Reproduce
Set `config.py`: `BENCH_ARCH`, `SAMPLES_PER_GROUP` (N = ×3), `FSX_ID`,
`REFERENCE_PATH`. `python run_headless.py`. See `../../README.md` and
`../../../docs/` (ported structure from aws-microbiome-demo).
