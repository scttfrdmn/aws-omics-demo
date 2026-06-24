# Methodology — how this benchmark is kept honest

This is the measurement study behind the demo: run a **real** germline
variant-calling pipeline (bcftools) on **real** 1000 Genomes Project low-coverage
WGS data, on x86 and on Graviton (arm64), and report the per-stage and end-to-end
time/data/cost — measured, not projected.

> **Status: harness ready — NOT YET RUN. No measurements exist.** This page is the
> *protocol* — the controls that make the numbers defensible once the study is run.
> The numbers themselves will land in [results.md](results.md); the story in
> [blog/end-to-end.md](blog/end-to-end.md).

## What we measure

The full from-scratch lifecycle, broken into phases, for each architecture:

```
provision (FSx + DRA)  →  stage (reference from canonical source)  →  run (@ N)  →  teardown
└──────────────── one-time, amortized over R runs ─────────────────┘     └─ per-run ─┘
```

For every phase: **wall-clock time, bytes moved, and cost** (instance
billed-seconds × on-demand $/hr, plus FSx GB-hours and S3 requests where they
matter). `N` (fan-out width = samples) is the tunable knob; runs are recorded as
`benchmark/results/lifecycle/<arch>-n<N>-fsx.json`.

## Fairness controls (only architecture may differ)

- **Same samples.** Both legs use the identical 1000 Genomes sample IDs (balanced
  N/3 per super-population: AFR / EUR / EAS).
- **Same instance spec, differ only in family.** `c7i↔c7g`, `r7i↔r7g` at identical
  vCPU/RAM. Never `c7i.2xlarge` vs `c7g.4xlarge` — that confounds architecture
  with size.
- **No burstable (t-family) instances anywhere in the measured path.** t4g/t3 are
  credit-based shared-core; the same workload varies ~2× with credit state, so
  you'd measure credit balance, not architecture. Every measured stage and the
  head node run on a fixed-performance family. (See memory: *no-t4g-for-benchmarking*.)
- **Same region/AZ** (us-east-1, pinned via `ext.az`) — same 1000 Genomes
  locality, same FSx mount locality.
- **Same pipeline** — both legs run the identical bcftools variant-calling
  Nextflow pipeline (same `bcftools`/`samtools` version, same `bcftools mpileup |
  bcftools call -mv` invocation, same chr20 region restriction).
- **Native on both legs.** x86 runs native amd64 containers
  (`quay.io/biocontainers/bcftools` / `samtools`); arm64 runs native arm64
  containers ([aarchbio](https://github.com/playgroundlogic/aarchbio) where the
  upstream image is amd64-only). Neither emulates — this is native-vs-native
  price/performance, the honest comparison. The emulation tax is a separate story.

## Instance pairs (the controlled variable)

| Stage | label | x86 leg | arm64 leg |
|-------|-------|---------|-----------|
| CALL_VARIANTS (per-sample fan-out) | process_medium | c7i.2xlarge | c7g.2xlarge |
| MERGE_VCFS (cohort fan-in) | process_high | r7i.2xlarge | r7g.2xlarge |
| VCF_STATS (cohort QC) | process_single | c7i.large | c7g.large |
| head node (Nextflow only) | — | c7i.large | c7g.large |

Variant calling has no memory-monster stage, so there is no heavy-tier
(`process_high_memory`) override: the shared reference rides FSx for every
CALL_VARIANTS task, which is the only stage that needs it.

## Timing: bill instance lifetime, not trace `realtime`

On the spawn executor, Nextflow's trace `realtime`/`duration` are **wrapper-local**
(sub-second even for tasks that ran minutes remotely). Per-stage timing comes from
**EC2 billed instance lifetime**, recovered from the head `.nextflow.log` (uploaded
to `results/<job>/nextflow.head.log`). Full rationale:
[decisions/0002](decisions/0002-timing-instance-lifetime-not-trace.md).

## The reference genome is staged from a canonical source, timed

- **Human reference `human_g1k_v37.fasta`** ←
  `s3://1000genomes/technical/reference/human_g1k_v37.fasta.gz` (public,
  in-region) → gunzip → `samtools faidx` → S3.

It is **not** laundered through old EBS snapshots or work-dirs — staging is a
measured cost with full provenance. The reference lands in S3, an S3-backed FSx for
Lustre filesystem imports it, and every CALL_VARIANTS task reads it in place (zero
copy). Why FSx and not EBS+FSR or per-task download:
[decisions/0001](decisions/0001-db-delivery-ami-ebs-fsx.md).

## Honesty requirements (enforced in the harness)

- **Per-stage, not just total** — CALL_VARIANTS (the fan-out + FSx-read stage) is
  the swing factor price alone can't predict.
- **Negative results stay in** — a stage slower on arm64 will be reported as such,
  never dropped.
- **Failed tasks invalidate a comparison** — any nonzero exit on either leg flags
  the run; a "faster" run that silently failed a step isn't faster. A clean A/B
  requires 0 failures on both legs.
- **Median + range, and the price/hr ratio is reported separately from the runtime
  ratio.** `$/run = price/hr × measured duration`; the price ratio (~19%) is fixed,
  the runtime ratio is what's measured.
- **N is stated on every result** — per-stage medians (n=N tasks) are robust,
  single-run per-stage *ratios* are flagged as such, and the cost delta is
  structural.
- **Variant-calling QC is validated** — see
  [decisions/0004](decisions/0004-variant-qc-titv-and-population-differentiation.md)
  for the planned Ti/Tv and population-differentiation checks.

## Validation science (what makes the calls credible)

Population-genetics QC instead of microbiome community structure:

- **Ti/Tv ratio** (transition/transversion) — the standard human variant-QC
  metric, ~2.0–2.1 genome-wide for WGS. A wildly off ratio flags a calling problem.
- **Variant counts, SNP/indel split** — from `bcftools stats`.
- **Population differentiation** — per-super-population allele frequencies; samples
  should cluster by super-population (within-population allele-frequency distance <
  between-population distance). The analog of within<between Bray–Curtis beta
  diversity in the microbiome study.

`analyze_study.py` is the harness that computes these. **No results exist yet.**

## The harness

| script | role |
|--------|------|
| `benchmark/build_fsx_db.py` | stage the reference genome from its canonical source onto FSx, timed (gated; `--plan`) |
| `benchmark/lifecycle_metrics.py` | record/render per-phase time·data·cost; one-time vs per-run + amortization |
| `benchmark/analyze_study.py` | per-stage arch timing (from head log) + variant-calling QC (Ti/Tv, population differentiation) |

See [`benchmark/README.md`](../benchmark/README.md) for exact run commands.
