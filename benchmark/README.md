# Benchmark harness — how to run it

The measurement study (Graviton vs x86, FSx-backed, full lifecycle) that this demo
was built to enable. This page is the **runbook**. For the *why* — fairness
controls, the timing trap, the reference-delivery decision, variant-QC validation —
see:

- **[../docs/methodology.md](../docs/methodology.md)** — protocol + fairness controls
- **[../docs/results.md](../docs/results.md)** — the results of record
- **[../docs/decisions/](../docs/decisions/)** — the design decisions
- **[results/lifecycle/MEASUREMENTS.md](results/lifecycle/MEASUREMENTS.md)** — raw running log

> **Status:** scaffolded — **NOT YET RUN**. No measurements exist. See [STATUS.md](STATUS.md).

## The scripts

| script | role | gated? |
|--------|------|--------|
| `build_fsx_db.py` | stage the human reference genome from its canonical source onto S3→FSx, timing every phase | `--plan` prints the plan + user-data; takes **no** action |
| `lifecycle_metrics.py` | record/render per-phase **time · data · cost**; one-time vs per-run + amortization | recorder/analyzer (pure stdlib) |
| `analyze_study.py` | per-stage arch timing (from head `nextflow.log`) + variant-calling QC (Ti/Tv, population differentiation) | analyzer |

## Running a lifecycle leg (per arch)

`N` (fan-out) is the knob: set `SAMPLES_PER_GROUP = N/3` in `config.py`. Everything
else (FSx, ECR pull-through cache, staged reference) is reusable across runs.

```bash
# 0. one-time: stage the reference from its canonical source onto FSx (gated — launch yourself)
python benchmark/build_fsx_db.py --plan      # prints the plan + the spawn commands
#    run the printed staging-instance command, then the printed --fsx-create command,
#    scope the DRA to the reference-fsx prefix, and set FSX_ID/FSX_MOUNT in config.py.

# 1. run the pipeline at N for this arch (set BENCH_ARCH = "arm64" or "x86" in config.py)
SAMPLE_COUNT=$N AWS_PROFILE=aws uv run python run_headless.py

# 2. pull the artifacts that survive instance teardown
aws s3 cp s3://$BUCKET/results/$JOB/nextflow.head.log benchmark/results/lifecycle/$ARCH-n$N/
aws s3 cp s3://$BUCKET/results/$JOB/trace.tsv          benchmark/results/lifecycle/$ARCH-n$N/
aws s3 cp s3://$BUCKET/results/$JOB/stats.json         benchmark/results/lifecycle/$ARCH-n$N/
aws s3 sync s3://$BUCKET/results/$JOB/variants/        benchmark/results/lifecycle/$ARCH-n$N/variants/

# 3. per-stage arch timing + variant-calling QC
python benchmark/analyze_study.py \
    --arm64-log   benchmark/results/lifecycle/arm64-n$N/nextflow.head.log \
    --x86-log     benchmark/results/lifecycle/x86-n$N/nextflow.head.log \
    --arm64-vcf   benchmark/results/lifecycle/arm64-n$N/variants \
    --x86-vcf     benchmark/results/lifecycle/x86-n$N/variants \
    --arm64-stats benchmark/results/lifecycle/arm64-n$N/stats.json \
    --x86-stats   benchmark/results/lifecycle/x86-n$N/stats.json \
    --json benchmark/results/lifecycle/study-n$N.json

# 4. render the end-to-end lifecycle report from a recorded leg
python benchmark/lifecycle_metrics.py benchmark/results/lifecycle/arm64-n$N-fsx.json
```

## Results layout

```
results/lifecycle/
  MEASUREMENTS.md            running log of every measured phase (the narrative)
  arm64-n<N>-fsx.json        per-leg lifecycle record (phases, time, data, cost)
  x86-n<N>-fsx.json
  staging_timings_*.json     reference staging phase timings
  qc_x86_n<N>.json           Ti/Tv + population differentiation + arch concordance
  arm64-n<N>/ , x86-n<N>/     head log, trace, per-sample VCFs, stats.json
```

The raw per-sample VCFs (`*/variants/`) are **gitignored** — large and regenerable.
The scientific conclusions are captured in `qc_x86_n<N>.json` + `MEASUREMENTS.md`;
re-pull the raw VCFs from the run's S3 results prefix (step 2 above) when you need
them for `analyze_study.py`.

## Fairness in one line

Only architecture differs: same samples, matched `c7i↔c7g`/`r7i↔r7g` pairs, **no
burstable instances** in the measured path, same region/AZ, same variant-calling
pipeline, native containers on both legs. Per-stage timing is **EC2 billed
lifetime**, not trace `realtime`. The full list is in
[../docs/methodology.md](../docs/methodology.md).
