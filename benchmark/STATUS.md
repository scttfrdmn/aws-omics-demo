# Status — SCAFFOLDED, NOT YET RUN

The x86-vs-Graviton variant-calling benchmark is **wired and ready, but has not
been run**. No measurements exist yet. The harness, pipeline, fairness protocol,
and analysis code are in place; running them on AWS is the next step.

- **How to run the harness:** [`README.md`](README.md)
- **Methodology + fairness controls:** [`../docs/methodology.md`](../docs/methodology.md)
- **What the study will measure (no numbers yet):** [`../docs/results.md`](../docs/results.md)
- **The story end-to-end (architecture + plan):** [`../docs/blog/end-to-end.md`](../docs/blog/end-to-end.md)
- **Why each design choice (decision records):** [`../docs/decisions/`](../docs/decisions/)

This repo was ported from [aws-microbiome-demo](https://github.com/scttfrdmn/aws-microbiome-demo)
(which IS run and measured — nf-core/taxprofiler on HMP data). The *shape*,
tooling (spawn / nf-spawn / truffle), and architecture carry over; the **science
is different** (germline variant calling on 1000 Genomes) and the **numbers do
not** — every measured value here must come from an actual run on this pipeline.

## To produce the results of record

1. Stage the reference genome onto FSx: `python benchmark/build_fsx_db.py --plan`
   (then run the printed, gated commands).
2. Run a lifecycle leg per arch (set `BENCH_ARCH`): `run_headless.py`.
3. Analyze: `python benchmark/analyze_study.py …` (per-stage timing + variant QC).
4. Record into `results/lifecycle/MEASUREMENTS.md` + per-leg JSON, then write up
   `docs/results.md` and the results section of the blog from the real numbers.
