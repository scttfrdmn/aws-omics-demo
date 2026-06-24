# The live demo — 1000 Genomes variant calling on real AWS compute

A live, talk-friendly demo: press **Start Analysis** and watch real EC2 instances
spin up, call germline variants with bcftools against real 1000 Genomes Project
low-coverage WGS data from the AWS Open Data registry, and surface the cost as it
happens — capped by a Bedrock Claude synthesis of the variants and population
genetics.

> This page is the **demo** (a presentation prop). The from-scratch measurement
> study — Graviton vs x86, FSx, lifecycle cost — lives in
> [methodology.md](methodology.md) and [results.md](results.md). The demo and the
> study share the same pipeline and code; they differ in reference-genome delivery
> (the demo bakes the reference into the AMI for a fast, self-contained cold start;
> the study uses shared FSx Lustre for wide fan-out — see
> [decisions/0001](decisions/0001-db-delivery-ami-ebs-fsx.md)).

## What happens during the demo

### Beat 1 — Start Analysis
1. **vCPU quota query** via [truffle](https://spore.host) — the account's actual
   EC2 quota sets the Nextflow `queueSize` (max concurrent tasks).
2. **Head node launch** — a single small instance launches via
   [spawn](https://spore.host), running Nextflow with the
   [nf-spawn](https://github.com/spore-host/nf-spawn) executor plugin.

### Beat 2 — Per-sample EC2 instances appear
Nextflow reads the samplesheet and dispatches tasks. For each 1000 Genomes sample,
**nf-spawn launches a dedicated EC2 instance** for `CALL_VARIANTS`, which:
- reads its aligned BAM **directly from the 1000 Genomes Open Data bucket**
  (`s3://1000genomes/phase3/data/`) — no staging, no copying, $0 data cost (same
  region, no egress);
- reads the shared **human reference genome** read-only off the FSx mount (the
  reference is staged once — see below);
- pulls the `bcftools`/`samtools` image (via the ECR pull-through-cached registry —
  see [decisions/0003](decisions/0003-fanout-ecr-ptc-and-stage-once.md));
- calls variants on chromosome 20 and writes the per-sample VCF to the S3 work
  directory, then self-terminates.

### Beat 3 — Merge + QC
Once the per-sample VCFs are in S3, the rest of the DAG runs, one EC2 instance per
task: **MERGE_VCFS** (`bcftools merge` — fan-in to one cohort VCF) → **VCF_STATS**
(`bcftools stats` → Ti/Tv ratio, SNP/indel counts). Intermediate data passes
through the shared S3 work dir — no instance talks directly to another.

### Beat 4 — Bedrock synthesis
When all samples complete, Bedrock Claude reads the cohort VCF stats and generates
plain-language insights about the variant calls and population genetics across the
three super-populations (AFR / EUR / EAS).

## Architecture

```
Local machine (FastAPI dashboard)
        │
        ▼
Head node  (Nextflow + nf-spawn plugin)
        │
        ├── CALL_VARIANTS ×N   ──► s3://1000genomes (aligned BAM)   no egress, $0 data
        │                      ──► reference genome (pre-staged on AMI for the demo)
        ├── MERGE_VCFS ×1
        └── VCF_STATS ×1
                │
                ▼
        S3 results bucket  →  Bedrock Claude  →  insights
```

Every task instance self-terminates after writing its outputs to S3.

## Prerequisites

```bash
brew install spore-host/tap/spawn   # spawn CLI — launches EC2 instances
brew install uv                     # Python package manager
```

AWS credentials configured as the `aws` profile (EC2, S3, Bedrock, EC2 Service
Quotas permissions).

## Setup (once before the talk)

```bash
cp config.example.py config.py      # set REGION, ACCOUNT_ID, BUCKET
make install
make ami        # bake the AMI (reference download + faidx is the bottleneck)
                # paste the printed AMI_ID into config.py
make demo-fake  # optional: rehearse the full pipeline with no AWS calls
```

## Run it

```bash
make demo       # opens http://127.0.0.1:8001 — press Start Analysis
```

Each `CALL_VARIANTS` instance stages its BAM from `s3://1000genomes`, indexes it,
and calls chromosome 20; the cohort then merges and the stats are computed. The
chr20-only restriction keeps the demo fast (the full genome would be ~40× the
mpileup work for the same architectural story).

## Teardown

```bash
make teardown   # stops instances, empties + deletes the S3 bucket
```

The AMI is **not** deleted automatically (EBS snapshot ≈ $2/month). Deregister
when done:

```bash
aws ec2 deregister-image --image-id <AMI_ID> --region us-east-1
```

## Cost

The 1000 Genomes data from the Open Data registry is **$0** (same-region, no
egress); only the ephemeral EC2 instances and Bedrock call cost money. The
measured, phase-by-phase cost breakdown and the Graviton comparison will live in
[results.md](results.md) once the study is run.

## Design FAQ

**Why the 1000 Genomes Open Data bucket?** The
[1000 Genomes Project](https://registry.opendata.aws/1000-genomes/) is hosted by
AWS in us-east-1; same-region EC2 reads its aligned BAMs at full S3 bandwidth with
no egress. Each CALL_VARIANTS instance reads its own sample's BAM independently —
no coordination, no staging. This is what a public data commons is for, and the
BAMs are **pre-aligned**, so there is no SRA fetch or realignment step.

**Why Nextflow + nf-spawn?** Nextflow gives the workflow DAG (dependencies, retries,
work-dir management); nf-spawn replaces the executor so each task gets its own
purpose-sized ephemeral EC2 instance that self-terminates the moment it's done —
per-second, per-task cost, no idle capacity, no queue to maintain.

**Why deliver the reference via FSx (in the study)?** The human reference genome
(`human_g1k_v37.fasta`, ~3 GB) is the one large, static, read-only thing every
CALL_VARIANTS task needs at once. At wide fan-out, a shared FSx Lustre filesystem
serves all N readers from one copy with no per-volume credit limit — see
[decisions/0001](decisions/0001-db-delivery-ami-ebs-fsx.md). The demo bakes it into
the AMI instead for the simplest self-contained cold start.

**Why chromosome 20 only?** The demo restricts variant calling to one contig
(`params.regions = '20'` — `human_g1k_v37` names it `20`, not `chr20`) so a full
fan-out finishes in minutes, not hours. The architecture (fan-out, FSx reference,
per-task instances) is identical to a whole-genome run; only the per-task compute
shrinks. chr20 is a standard demo/QC contig: large enough for a meaningful Ti/Tv
and variant count, small enough to be fast.

**Why Graviton?** `c7g`/`r7g` deliver comparable or better price/performance vs
`c7i`/`r7i` at the same vCPU/RAM, ~19% cheaper per hour. Nextflow, Docker, and the
bcftools/samtools containers support arm64. The full measured comparison will be in
[results.md](results.md) once run.
