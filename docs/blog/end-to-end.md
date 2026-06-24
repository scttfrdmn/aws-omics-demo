# Germline variant calling on AWS Graviton: one instance per sample, a shared reference, and a Graviton price/performance test

*We built a 1000 Genomes variant-calling pipeline on AWS that launches one
ephemeral EC2 instance per sample via spawn/nf-spawn, reads pre-aligned BAMs
straight from the public 1000 Genomes bucket at $0 egress, and shares a single
human reference genome off FSx Lustre to every task. It's wired to run on x86 and
Graviton for a head-to-head. Here's the science, the tooling, and the comparison
we're set up to measure.*

> **Status: harness ready, not yet run.** The architecture, pipeline, and
> measurement harness are in place. The "what we'll measure" sections below describe
> planned outputs in the conditional tense — **no numbers are filled in**, because
> nothing has been run yet. This study's *shape* is ported from
> [aws-microbiome-demo](https://github.com/scttfrdmn/aws-microbiome-demo) (which is
> measured); the science here is different, so none of its numbers carry over.

---

## The science

The [1000 Genomes Project](https://www.internationalgenome.org/) is the canonical
public catalogue of human genetic variation — thousands of individuals across
26 populations, with openly available sequencing data. A working variant-calling
pipeline, given the aligned reads, should rediscover germline variants that pass
standard quality checks and reflect known population structure.

So that's the test. We take **N samples balanced across three super-populations**
— **AFR** (African), **EUR** (European), **EAS** (East Asian) — and run their
pre-aligned, low-coverage BAMs through a compact `bcftools` variant-calling
pipeline, restricted to **chromosome 20** for demo speed:

```
CALL_VARIANTS (per sample)  →  MERGE_VCFS (cohort)  →  VCF_STATS (QC)
```

- **CALL_VARIANTS** — `bcftools mpileup -f <reference> -r 20 <bam> | bcftools call -mv -Oz`,
  one task per sample. This is the fan-out stage, and the one that reads the shared
  reference genome.
- **MERGE_VCFS** — `bcftools merge` combines all per-sample VCFs into one cohort VCF.
- **VCF_STATS** — `bcftools stats` produces the QC metrics (Ti/Tv, SNP/indel counts).

The BAMs come straight from the
[1000 Genomes data on the AWS Open Data registry](https://registry.opendata.aws/1000-genomes/)
(`s3://1000genomes/phase3/data/`), hosted in us-east-1. Same-region EC2 reads them
at full S3 bandwidth with **no egress charge** — each task pulls its own sample
directly, no central staging. This is what a public data commons is for.

## What "recovers the biology" will mean here

Once run, the pipeline's calls get checked against standard population-genetics
expectations (the variant-calling analog of microbiome community structure):

- **Ti/Tv ratio** — transitions vs transversions, expected **~2.0–2.1** genome-wide
  for human data. A ratio far below that signals false-positive noise. *(planned)*
- **Variant counts** — total SNPs and indels called on chr20 across the cohort. *(planned)*
- **Population differentiation** — per-super-population allele frequencies should
  cluster by group: within-group allele-frequency distance **< between-group**. That
  separation is the variant-calling counterpart of body sites separating by
  community. *(planned)*

The method and expected ranges are written up in
[decision record 0004](../decisions/0004-variant-qc-titv-and-population-differentiation.md).
The numbers get filled into [results.md](../results.md) from the actual run — never
by hand.

---

## How it runs: one right-sized instance per task, with spore.host tooling

The thing that makes this architecture clean is that **there is no cluster and no
queue to manage.** Nextflow provides the workflow DAG — task dependencies, retries,
work-directory management — and the [spore.host](https://spore.host) toolchain
replaces the executor so that **every task gets its own ephemeral EC2 instance**,
sized for that task, which self-terminates the instant it finishes.

Three tools, three jobs:

- **[nf-spawn](https://github.com/spore-host/nf-spawn)** — the Nextflow executor
  plugin (`id 'nf-spawn@0.8.0'`). Instead of submitting to Batch or Slurm, each
  Nextflow process launches a dedicated instance. Per-process directives steer
  placement and storage: `ext.az` pins a task to an availability zone, and
  `ext.fsx = [id, mount, paths]` mounts a shared FSx filesystem and zero-copy
  symlinks the reference genome into the task's work dir.
- **[spawn](https://spore.host)** — the CLI nf-spawn calls to launch, tag, and reap
  instances (and to create and attach the FSx filesystem). One instance per task,
  billed per second, no idle capacity.
- **[truffle](https://spore.host)** — queries the account's real EC2 vCPU quota,
  spot pricing, and instance specs. The pipeline derives Nextflow's `queueSize`
  (how wide to fan out) from the *actual* quota, and uses truffle's pricing to cost
  every stage.

Concretely: each `CALL_VARIANTS` task lands on its own c-family instance, reads its
one BAM from `s3://1000genomes` and the shared reference from FSx, calls variants,
writes its VCF to S3, and terminates. The cohort `MERGE_VCFS` step takes a
larger r-family box; `VCF_STATS` a small one. Sizing is per task, not per cluster.

```
Nextflow (head)  ── nf-spawn executor ──►  spawn ──►  one EC2 instance per sample
   │                                                      │
   │   truffle: quota → queueSize, pricing → cost         ├── reads its BAM from s3://1000genomes ($0 egress)
   │                                                      ├── reads the shared reference off FSx Lustre (zero copy)
   └── DAG, retries, work-dir on S3                        └── writes its VCF to S3, self-terminates
```

The shared-reference design carries the fan-out. Every `CALL_VARIANTS` task needs
the same **human reference genome (~3 GB)** at once, so it lives on **one S3-backed
[FSx for Lustre](https://aws.amazon.com/fsx/lustre/) filesystem**, mounted read-only
by every task via `ext.fsx`. The reference is staged once (from
`s3://1000genomes/technical/reference`, `gunzip` + `samtools faidx`); tasks read it
in place with zero per-task copying. The trade-offs behind FSx Lustre vs. baking the
reference into an AMI vs. per-task download are in
[decision record 0001](../decisions/0001-db-delivery-ami-ebs-fsx.md).

---

## x86 vs Graviton: the comparison we're set up to make

This is the part the whole thing is built to measure. Both architectures will run
the **identical pipeline, identical samples, identical reference**, native on each
chip — x86 on `c7i`/`r7i` with amd64 containers, Graviton on `c7g`/`r7g` with native
arm64 `bcftools`/`samtools` containers ([aarchbio](https://github.com/playgroundlogic/aarchbio)
rebuilds). Neither emulates. The only variable is the processor: matched vCPU/RAM
pairs (`c7i↔c7g`, `r7i↔r7g`), no burstable instances anywhere in the measured path,
same region and AZ.

### Correctness first

Before any speed or cost claim, the calls must be **architecture-independent** —
the same variants at the same sites on both chips. The genome doesn't change with
the processor; confirming that is the precondition for everything else.

### The hypothesis

The expectation — to be **confirmed or refuted by measurement**, not assumed:

- **Runtime roughly at parity** per-stage on matched instance pairs.
- **Graviton cheaper**, because it's ~19% lower $/hr at matched vCPU/RAM.
- **Net: near-equal throughput at lower cost.**

Whether variant calling holds that pattern is an open question worth testing
directly: `bcftools mpileup` is more I/O- and memory-bandwidth-bound than the
CPU-bound classifiers in the source metagenomics study, so the per-stage ratios
could land differently. That's exactly why we measure rather than project. The
per-stage timing will be billed by **actual EC2 instance lifetime** (not Nextflow
trace `realtime`, which is wrapper-local on the spawn executor — see
[decision record 0002](../decisions/0002-timing-instance-lifetime-not-trace.md)).

Result tables live in [results.md](../results.md) as templates until the run fills
them in.

---

## Run it

`N` (fan-out width) is a one-line knob (`SAMPLES_PER_GROUP`); the shared filesystem,
the staged reference, and the container cache are all reusable across runs and
architectures. Flip `BENCH_ARCH`, point at your bucket, and turn the dial. The
runbook is in [`benchmark/README.md`](../../benchmark/README.md); the fairness
protocol is in [methodology.md](../methodology.md); the design decisions (reference
delivery, timing method, variant-QC validation) are written up as
[decision records](../decisions/).

*Built on [Nextflow](https://www.nextflow.io/) · `bcftools`/`samtools` ·
[spawn / nf-spawn / truffle](https://spore.host) ·
[aarchbio](https://github.com/playgroundlogic/aarchbio) native arm64 containers ·
[1000 Genomes on AWS](https://registry.opendata.aws/1000-genomes/) · Amazon Bedrock.*
