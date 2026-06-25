# `pipeline/` — the Nextflow pipeline and how it runs on AWS

This directory holds the **actual pipeline**, as readable files you can open, run,
and reason about — not strings buried in Python. If you want to understand what
the demo computes, start with [`main.nf`](main.nf).

```
pipeline/
  main.nf                 the Nextflow DAG: CALL_VARIANTS → MERGE_VCFS → VCF_STATS
  nextflow.config         (generated per-run by ../nextflow_config.py — see below)
  monitor.py              head-side progress reporter (writes progress.json)
  head_bootstrap.sh.tmpl  cloud-init for the Nextflow head instance
  README.md               this file
```

## The science (`main.nf`)

Germline variant calling on **1000 Genomes** low-coverage WGS, restricted to
**chromosome 20** for demo speed:

| stage | tool | what it does |
|-------|------|--------------|
| `CALL_VARIANTS` | `bcftools mpileup \| call -mv` | per sample (parallel fan-out): call SNPs+indels on chr20 |
| `MERGE_VCFS` | `bcftools merge` | combine all per-sample VCFs into one cohort VCF |
| `VCF_STATS` | `bcftools stats` | cohort QC → `stats.json` (Ti/Tv, SNP/indel counts) |

The key sanity check is the **Ti/Tv ratio** (~2.0–2.1 for real human calls). A
validated N=3 run produced 154,877 variants at Ti/Tv 2.21.

`main.nf` is a normal Nextflow file — you can run it anywhere Nextflow runs, with
any executor, given a `samples.csv` and a reference at `<fsx_mount>/reference`.

## How it runs on AWS: spore.host + nf-spawn

There is **no cluster and no job queue**. The execution model is:

```
run_headless.py / app.py
        │  (spawn launch)
        ▼
  Head instance ── Nextflow + nf-spawn executor ──┐
        │                                          │ one EC2 instance PER TASK
        │   head_bootstrap.sh.tmpl sets it up      ▼
        │                                   ┌──────────────────────┐
        │                                   │ CALL_VARIANTS (×N)   │ c7g/c7i.2xlarge
        │                                   │  reads BAM ◄─ s3://1000genomes ($0 egress)
        │                                   │  reads ref ◄─ /fsx (FSx Lustre, zero-copy)
        │                                   │  writes VCF ─► s3://…/work/
        │                                   └──────────────────────┘
        │                                   ┌──────────────────────┐
        │                                   │ MERGE_VCFS  → VCF_STATS│ r7g/c7g
        │                                   └──────────────────────┘
        ▼  each task instance self-terminates the moment its task finishes
  progress.json / summary.json (monitor.py) ─► dashboard
```

- **[spawn](https://spore.host)** launches/reaps the EC2 instances.
- **[nf-spawn](https://github.com/spore-host/nf-spawn)** is the Nextflow executor
  plugin that turns each Nextflow process into a spawn instance. Per-process
  directives in `nextflow.config` control it:
  - `ext.instanceType` — the instance for that process's `label`
  - `ext.az` — pin to one AZ (so the FSx mount + any FSR-warmed volume are local)
  - `ext.fsx` — mount the shared FSx Lustre filesystem (the reference genome)
  - `ext.ttl` — auto-terminate guard
- **[truffle](https://spore.host)** (in `../truffle.py`) queries the account's real
  vCPU quota to set Nextflow's `queueSize` (fan-out width) and prices each stage.

### The one wrinkle worth understanding: `CALL_VARIANTS` has no `container` directive

`CALL_VARIANTS` does two kinds of work — **host-level S3 I/O** (`aws s3 cp`, which
needs the AWS CLI baked into the host AMI) and the **bio-tool step** (`bcftools`,
which lives in a container). A Nextflow `container` directive would run the
*entire* process script inside the bcftools image, which has no AWS CLI. So the
script stays on the host and calls bcftools via an explicit `docker run` (mounting
both the work dir and the read-only FSx reference). `MERGE_VCFS` and `VCF_STATS`
are pure bcftools, so they use the normal `container` directive. This split is the
standard spore.host pattern for "touch S3 **and** run a single-tool BioContainer".

### The reference genome is read directly off FSx (zero-copy)

The human reference (`human_g1k_v37.fasta` + `.fai`, ~3 GB) is staged once into a
shared S3-backed FSx Lustre filesystem (see `benchmark/build_fsx_db.py`). nf-spawn
mounts that filesystem on every `CALL_VARIANTS` instance, and the process reads
`<fsx_mount>/reference` in place — it is never copied per task. This is the
wide-fan-out reference-delivery story (FSx, not per-task download, not a per-task
EBS volume) — see `docs/decisions/0001`.

## `nextflow.config` is generated, not stored here

The config is **per-run dynamic** — instance types switch with `BENCH_ARCH`
(c7g/r7g on Graviton vs c7i/r7i on x86), `queueSize` comes from the live truffle
quota query, the AZ pin and FSx id come from `config.py`, and the arm64 container
overrides are emitted only for the Graviton leg. That logic lives in
[`../nextflow_config.py`](../nextflow_config.py) (`render()`), which writes a
`nextflow.config` and uploads it next to `main.nf` for each run. Run
`python -c "import config, omics_demo.nextflow_config as n; print(n.render(config, 8))"`
to see the config for your current `config.py`.

## How these files reach the head node

`../worker_script.py` is a thin shipper: it renders `head_bootstrap.sh.tmpl`
(substituting `@@TOKEN@@` values) and uploads `main.nf`, `monitor.py`, and the
generated `nextflow.config` to S3. The head fetches them on boot and runs
`nextflow run main.nf`. No pipeline logic lives in the Python — it only moves
these files and fills in run-specific values.
