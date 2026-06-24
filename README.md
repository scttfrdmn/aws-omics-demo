# 1000 Genomes variant calling on AWS — live demo + Graviton benchmark

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![AWS Graviton](https://img.shields.io/badge/AWS-Graviton-orange.svg)](https://aws.amazon.com/ec2/graviton/)
[![spawn](https://img.shields.io/badge/powered%20by-spawn-5c5cff.svg)](https://spore.host)

Run a **real** germline variant-calling pipeline (bcftools) on **real** 1000
Genomes Project low-coverage WGS data on AWS — one ephemeral EC2 instance per
task via [Nextflow](https://www.nextflow.io/) +
[nf-spawn](https://github.com/spore-host/nf-spawn), reading the aligned BAMs
straight from the AWS Open Data registry at $0 egress.

This repo is **two things** built on the same pipeline and code:

| | What it is | Start here |
|---|------------|-----------|
| 🎤 **The live demo** | A talk-friendly FastAPI dashboard: press *Start Analysis*, watch instances appear, see cost accrue, finish with a Bedrock Claude synthesis of the variants and population genetics. | **[docs/demo.md](docs/demo.md)** |
| 📊 **The benchmark study** | A from-scratch, fully-instrumented measurement: Graviton (arm64) vs x86, an FSx-backed reference genome, time·data·cost for every lifecycle phase, with fan-out `N` as the knob — *how to do this properly*. | **[docs/results.md](docs/results.md)** |

## What the study will measure (harness ready — NOT YET RUN)

This demo has **never been run**; there are no measurements yet. The harness is
built to produce, per architecture and per fan-out `N`:

- **Per-stage runtime** (CALL_VARIANTS / MERGE_VCFS / VCF_STATS) from EC2 billed
  instance lifetime, with a Mann–Whitney U test on the arm64/x86 delta.
- **End-to-end cost**, broken into one-time (FSx provision + reference staging)
  vs per-run, with arm64-vs-x86 $/hr held separate from the runtime ratio.
- **Reference-genome delivery at wide fan-out** — whether shared FSx Lustre
  carries many concurrent CALL_VARIANTS readers where EBS+FSR hits a credit cliff.
- **Variant-calling QC validates:** a genome-/region-expected **Ti/Tv ratio**
  (~2.0–2.1 for human WGS) and **population differentiation** — samples should
  cluster by super-population (within-population allele-frequency distance <
  between-population).

These are **planned outputs**, stated in the future tense until the run happens.
Full breakdown → **[docs/results.md](docs/results.md)**.

## Documentation map

```
docs/
  demo.md            ← run the live demo (setup, AMI, teardown, design FAQ)
  methodology.md     ← how the benchmark is kept honest (fairness controls, protocol)
  results.md         ← canonical results: lifecycle cost, arch comparison, QC (not yet run)
  blog/end-to-end.md ← the full story: architecture + the sharp edges
  decisions/         ← decision records (the "why" behind each design choice — see README)
    0001 reference-genome delivery: AMI → EBS+FSR → FSx Lustre
    0002 timing: bill instance lifetime, not trace realtime
    0003 fan-out: ECR pull-through cache + stage reference data once
    0004 variant QC: Ti/Tv + population differentiation (planned validation)
  ami-vs-data-volume.md  ← earlier design note on reference-on-AMI vs data volume
benchmark/
  README.md          ← how to run the measurement harness
  *.py               ← harness (build_fsx_db, lifecycle_metrics, analyze_study, diff_traces)
  results/lifecycle/ ← results of record (MEASUREMENTS.md + per-leg JSON)
  results/_archive/  ← superseded N=3 EBS+FSR pilots, kept for provenance
```

## Quick start

**Live demo:**
```bash
cp config.example.py config.py   # set REGION, ACCOUNT_ID, BUCKET
make install
make demo-fake                   # rehearse with no AWS calls
make demo                        # → http://127.0.0.1:8001, press Start Analysis
```
Full instructions, AMI bake, and teardown: **[docs/demo.md](docs/demo.md)**.

**Benchmark study:** see **[benchmark/README.md](benchmark/README.md)** and
**[docs/methodology.md](docs/methodology.md)**.

## Built on

[Nextflow](https://www.nextflow.io/) · a custom bcftools variant-calling pipeline ·
[spawn + nf-spawn + truffle](https://spore.host) ·
[aarchbio](https://github.com/playgroundlogic/aarchbio) (native arm64 containers) ·
[1000 Genomes Open Data on AWS](https://registry.opendata.aws/1000-genomes/) ·
Amazon Bedrock.
