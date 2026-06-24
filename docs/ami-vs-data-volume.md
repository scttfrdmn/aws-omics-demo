# Design note: how task instances get the reference genome (AMI vs. data volume)

Status: design discussion. Captures the tradeoffs and a recommended direction.

## The problem this addresses

Each pipeline task runs on its own ephemeral EC2 instance (via spawn / nf-spawn),
and the per-sample CALL_VARIANTS stage fans out N-wide per run. The genuinely
large, static thing those instances need is the **human reference genome —
`human_g1k_v37.fasta`, ~3 GB uncompressed, plus its `.fai` index**. Everything else
a task needs (Nextflow, Docker, the spawn CLI, the nf-spawn plugin, the
bcftools/samtools container) is small and fast to install or is already pulled at
runtime.

The custom-AMI option welds together three things:
1. the OS (AL2023, per-arch),
2. the tools/pipeline, and
3. the reference genome on the root volume.

The custom AMI is built and snapshotted via `spawn launch` + `spawn ami create`.

## Why the AMI approach is defensible

- **Repeated fan-out amortizes the bake.** One-time bake, then every task across
  every run skips the reference fetch + `faidx` + tool install. For an
  N-instance-per-run, run-repeatedly workload with a stable reference, this is the
  AMI's sweet spot.
- Fastest possible cold start for a CALL_VARIANTS task — the reference is already
  on the root volume at boot.

## Why it's awkward (the friction the pattern hits)

The custom AMI does **two jobs welded together** — "carry a multi-GB data blob" and
"be the machine image" — and the coupling causes real pain:
- An **arch split**: separate x86 and arm64 AMIs, each a separate bake.
- Reference or tool updates force a **full machine-image rebuild**, not a data
  update.
- You own AMI version drift and per-arch detection, instead of leaning on the stock
  AL2023 image spawn auto-detects.

## The better-factored direction (recommended): reference on its own EBS volume

Put the reference genome on a **dedicated, right-sized EBS volume** (created once
from a snapshot), and let task instances mount it read-only on a **stock AL2023
AMI**. Then:

- **Right-size each volume independently.** The reference volume is sized for the
  reference (~8 GB). The spore **root** volumes shrink to just what a task needs
  (OS + container + the BAM it stages from S3) instead of carrying the reference
  baked into every root snapshot.
- **Decouple data from machine.** Update the reference by re-snapshotting one
  volume; no AMI rebuild. Base image stays the stock AL2023 spawn auto-detects — no
  custom AMI, no arch-split bake.
- **More spawn-idiomatic.** Lean on `--ami auto` + an attached data volume rather
  than handing spawn a hand-maintained custom AMI.

## Option comparison

| | Custom AMI | Stock AMI + reference on EBS snapshot/volume | Stock AMI + EFS/FSx for reference | Stock AMI + per-task S3 fetch |
|---|---|---|---|---|
| CALL_VARIANTS cold-start | Fastest (reference on root) | Fast (attach pre-populated volume) | Fast-ish (network FS; FSx-Lustre good for read-heavy) | Slowest (~3 GB fetch + faidx per cold task) |
| Reference update | Full AMI rebake | Re-snapshot one volume | Update the FS copy | Change S3 key |
| Volume sizing | Every root carries the reference | Reference volume + small spore roots, each right-sized | Small roots; reference on shared FS | Small roots; reference transient |
| Base image | Custom AMI per arch (drift, rebake) | Stock AL2023 (`--ami auto`) | Stock AL2023 | Stock AL2023 |
| Best when | Repeated fan-out, stable reference, task-side simplicity | Repeated fan-out, reference/tools evolve independently | Reference shared across many concurrent readers; very large refs | One-off / infrequent runs |

## Recommendation

For a **fixed demo** with a stable reference and a wide fan-out, baking the
reference into the AMI is the lowest-config option. For the **benchmark study** and
any wide-fan-out production run, deliver the reference via shared **FSx for Lustre**
so all N CALL_VARIANTS readers share one copy with no per-volume credit limit (see
[decisions/0001](decisions/0001-db-delivery-ami-ebs-fsx.md)). The EBS-volume path is
the middle ground — small right-sized roots, decoupled data, no rebakes — best when
N is modest.

Interim fallback that needs no upstream change: a stock AMI where the CALL_VARIANTS
task script fetches the reference from S3 itself (the same self-fetch pattern the
task already uses for its BAM from `s3://1000genomes`) — at the cost of the ~3 GB
pull + `faidx` on cold tasks.

## Building & serving the reference volume efficiently (EBS direct APIs + FSR)

Two AWS capabilities make the "reference on its own EBS volume" path materially
better than a naive snapshot, and split cleanly across the build side and the read
side.

### Build side — create the snapshot WITHOUT a bake instance (EBS direct APIs)

The [EBS direct APIs](https://docs.aws.amazon.com/ebs/latest/APIReference/Welcome.html)
let you create and populate a snapshot directly, no EC2 instance and no attached
volume:

- `StartSnapshot` → `PutSnapshotBlock` (×N) → `CompleteSnapshot` — write the
  reference straight into a snapshot from a laptop or a Lambda.
- `ListChangedBlocks` / `ListSnapshotBlocks` / `GetSnapshotBlock` — read and diff
  snapshot blocks.

Impact: building the reference snapshot no longer needs a
launch→download→`faidx`→`spawn ami create`→terminate dance. And it's
**incremental** — a reference version bump writes only changed blocks
(`ListChangedBlocks`), not a fresh image.

`spawn snapshot create --from <dir|.tar.gz|raw>` (spawn ≥ 0.48.0) packs a
directory/tarball into an ext4 image in-process (pure Go, no `mkfs`, no builder
instance) and streams it into the snapshot. Example:
```
spawn snapshot create --from s3://1000genomes/technical/reference/human_g1k_v37.fasta.gz \
    --size 8 --name human-g1k-v37 --region us-east-1
```

**Where you run the build matters.** Running this from a laptop is slow and
RAM-heavy: spawn assembles the full ext4 image in memory before/while uploading,
and the data path is S3 → laptop → EBS over the home uplink. **Recommendation:
build the snapshot from a small EC2 instance / Lambda in the target region**, so
S3 → snapshot stays in-region (fast, ample RAM). The build is a **one-time,
amortized** cost regardless of where it runs.

### Read side — Fast Snapshot Restore so wide fan-out isn't slow

A volume created from a snapshot **lazy-loads blocks from S3 on first access**, so
the first reference read on each fresh task pays that latency — and for an N-way
fan-out, N volumes-from-the-same-snapshot all do. **Fast Snapshot Restore (FSR)**
pre-warms the snapshot so every volume created from it is immediately at full
performance. FSR is the ingredient that makes the data-volume path match the
baked-AMI cold-start speed for wide fan-out (it has a per-AZ hourly cost while
enabled — worth it during a run, disable after).

### Why this is also a general spawn primitive

This isn't reference-genome-specific. "Materialize reference data into an EBS
snapshot via the direct APIs, then attach (FSR-warmed) volumes from it to launched
instances" is a clean, reusable way to get any large reference (reference genomes,
BLAST/bowtie2 DBs, ML model weights, …) onto ephemeral spores without baking AMIs
or running a bake instance.

## Tagging reference snapshots

These reference snapshots live in the account long-term and get attached to many
launches — so they need provenance tags, or they become mystery blobs.
`spawn snapshot create` applies a baseline (`Name`, `spawn:snapshot-name`,
`spawn:managed`, `spawn:source=ebs-direct`) but has no `--tag` flag yet
(spawn#161). Until it does, `scripts/tag_db_snapshot.sh` applies a consistent
schema after each build:

| tag | meaning |
|-----|---------|
| `project` | `aws-omics-demo` |
| `role` | `reference-genome` |
| `tool` | `bcftools` / `samtools` |
| `ref` | short reference name (`human_g1k_v37`) |
| `ref-version` | exact version (`human_g1k_v37`) |
| `source` | where it came from (the 1000 Genomes S3 URI) |
| `mount` | where the task mounts it (`/opt/reference`) |
| `built-by` / `built-date` | `spawn-snapshot-create` + date |

Run it as: `scripts/tag_db_snapshot.sh <snap-id> <tool> <ref> <ref-version> <source> <mount>`.

## How volume-backed references work end-to-end (zero-copy, no pipeline fork)

The reference is read directly off a read-only EBS volume (or the shared FSx
mount) — no baked reference, no per-task download, unmodified pipeline. The recipe:

1. `spawn snapshot create --from s3://…/human_g1k_v37.fasta.gz --size N --name human-g1k-v37 --tag …`
   (instance-free, bounded memory; build in-region for the large reference). Tag
   for provenance.
2. Attach the **same read-only snapshot on the head AND every task** at the same
   mount path. Head: nf-spawn's launcher passes `--attach-volume` (here via
   `HEAD_ATTACH_VOLUMES` in `spawn.launch_workers`), so the head mounts it too and
   the pipeline's head-side `reference` *exists* validation passes. Tasks:
   per-process `ext.volumes`.
3. The reference `db_path`/marker is the mount path; the **mount basename must
   equal the input stage name** so nf-spawn symlinks instead of copying.

### What made it work (the upstream arc)

The naive attempt failed for real reasons, each fixed upstream:
- **spawn#166** — `--attach-volume` mounted *after* the user-data workload; now
  mounts before, so head-side validation sees the reference.
- **nf-spawn#49 → #51 (0.5.0) → #55/#56 (0.6.0)** — when the pipeline *stages* the
  reference path, nf-spawn saw the Nextflow S3 stage copy as the input source, not
  the mount, and fell back to `aws s3 cp` (per-task download). 0.6.0 matches the
  input's **stage-name basename** to an attached `ext.volumes` mount and
  **symlinks** it (skip the copy) regardless of source URI → genuine zero-copy.

### Sizing note (not an nf-spawn issue)

Variant calling has no resident-RAM monster — `bcftools mpileup` against one
chromosome is modest. The reference volume removes the *copy*, not a large RAM
footprint, and the CALL_VARIANTS instance (`c7g/i.2xlarge`) has ample headroom. (In
the microbiome predecessor, MetaPhlAn needed a memory-optimized override; there is
no analog here.)

### Still standing as the simpler fallback

Baking the reference into the AMI still works and is the lowest-config option for a
stable reference + wide fan-out; the EBS-volume path wins when the reference changes
independently of the image or you want small, right-sized root volumes and no
rebakes; FSx wins for wide fan-out (no per-volume credit cliff).

### Tracking

- **nf-spawn#45** — `ext.volumes` / snapshot-mount per process (the read/attach side).
- **nf-spawn#47** — per-task setup hook (so stock AL2023 works without a tools AMI).
- **spawn#147** — build-snapshot-from-S3 (EBS direct APIs) + attach FSR-warmed
  volume-from-snapshot (the general primitive).
- **spawn#157** — `snapshot create` streams (bounded memory) instead of buffering.
- **spawn#161** — `--tag k=v` on `snapshot create` / `launch` for provenance tags.
- **nf-spawn#49 → #51 (0.5.0) → #55/#56 (0.6.0)** — symlink a staged `path` input to its
  `ext.volumes` mount (by stage-name basename) instead of copying → zero-copy.
- **spawn#166** — `--attach-volume` mounts before the user-data workload (head-side validation).
