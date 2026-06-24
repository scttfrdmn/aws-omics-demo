# 0001 — Reference-genome delivery: AMI → EBS+FSR → FSx Lustre

**Status:** accepted (FSx Lustre is the answer for wide fan-out)
**Date:** 2026-06-17

## Context

Every pipeline task runs on its own ephemeral EC2 instance (one instance per
Nextflow task, via spawn / nf-spawn). The per-sample CALL_VARIANTS stage needs a
large, static reference:

- **Human reference genome `human_g1k_v37.fasta`** — ~3 GB uncompressed, plus its
  `.fai` index, read by `bcftools mpileup` on every sample.

At a fan-out of N samples, *many* CALL_VARIANTS instances need the same reference
at the same time. How that reference reaches each instance is the single biggest
cost/throughput lever in the whole pipeline. We considered three approaches, in
order.

## Option A — bake the reference into the AMI

Weld OS + tools + the reference genome onto one custom AMI.

- ✅ Fastest cold start (reference already on root volume, zero copy at run time).
- ✅ No per-run data movement.
- ❌ Bigger AMI snapshot; a reference refresh means a re-bake.
- ❌ Couples the data blob to the machine image, and forces an arch-split bake.

Fine for a fixed demo. Too rigid once the reference is a moving part and you want
to swap reference versions without re-baking.

## Option B — per-task S3 download

Each CALL_VARIANTS task `aws s3 cp`s the reference from S3 to its own disk at
startup, then `samtools faidx`.

- ❌ **The worker is running and billed while it copies + indexes.** Copy/faidx
  time is wasted compute-$ on an instance, paid N times. For a ~3 GB reference
  that's a per-task download + faidx multiplied by the fan-out.

Rejected: it converts cheap data movement into expensive idle compute.

## Option C(i) — EBS snapshot + Fast Snapshot Restore (FSR)

Pre-build the reference onto an EBS volume, snapshot it, and have each task attach
a volume restored from that snapshot. nf-spawn `ext.volumes` + the #55 zero-copy
symlink mean the task reads the reference in place — no copy.

This *worked at N=3* and was clean in the predecessor study. Two sharp edges
surface scaling up:

1. **Un-warmed FSR volumes lazy-load at ~6–8 MB/s** — the first read of each block
   faults in from S3. A multi-GB reference read cold is slow; the caller crawls.
   *Fix:* enable FSR so volumes are pre-warmed (`FastRestored=true`).
2. **FSR is per-AZ**, so tasks must be pinned to the FSR-enabled AZ
   ([`ext.az`](https://github.com/spore-host/nf-spawn) — nf-spawn#62, released
   0.7.0). Without pinning, tasks land in other AZs and get cold volumes.

Then the wall at scale:

3. **FSR has a ~10-volume credit bucket per snapshot.** At wide N, the volumes
   restored from the snapshot **drain the FSR credits** → most volumes are NOT
   fast-restored → back to ~6–8 MB/s lazy-load. EBS+FSR has a hard ceiling around
   ~10 concurrent fast-restored readers. This is the **FSR credit cliff**.

## Decision — FSx for Lustre (Option C(ii))

One S3-backed FSx for Lustre filesystem (PERSISTENT_2, 1200 GiB, 250 MB/s/TiB),
populated once from S3 via a Data Repository Association, mounted **read-only by
all N task instances**.

- ✅ **No per-volume credit bucket → no cliff.** The shared FS carries many
  concurrent readers — the exact thing EBS+FSR couldn't do.
- ✅ The reference lives once on the shared FS; it never copies per task.
- ✅ Throughput is a dial (125–1000 MB/s/TiB), not a credit balance.
- ✅ Single-AZ FS → still pin tasks to that AZ (`ext.az`), but for *locality*, not
  credit conservation.

Cost: FSx bills storage-GB-hours while it exists (~$0.24/hr for 1200 GiB). That's
a one-time/amortized cost per benchmark, not per-run-per-sample, and spawn's
ttl-reaper reclaims orphaned filesystems.

### Wiring (so it stays reproducible)

- nf-spawn `ext.fsx = [id:'fs-…', mount:'/fsx', paths:['reference']]` per
  CALL_VARIANTS task (forwarded to `spawn launch --fsx-id`), released in
  **nf-spawn 0.8.0** (#67). `paths` is **required** for the #55 zero-copy symlink —
  without it nf-spawn only exposes bare `/fsx` and the staged reference path gets
  copied instead of symlinked.
- the reference `db_path`/marker in the samplesheet is an **`s3://` marker**, not a
  head-local path — otherwise Nextflow's FilePorter bulk-copies the reference on
  the head before any task runs (the nf-spawn#65 deadlock). The head only needs
  `exists:true`; the tasks read off `/fsx`.

## Decision matrix

| approach | copy on worker | $ during copy | scaling limit |
|----------|---------------|---------------|---------------|
| A — baked AMI | none | $0 | reference refresh = re-bake |
| B — per-task S3 download | full reference + faidx | **wasted compute × N** | none, but expensive |
| C(i) — EBS + FSR | none (symlink) | ~$0 | **~10-reader FSR credit cliff** |
| **C(ii) — FSx Lustre** | none (symlink) | ~$0 | **FSx throughput dial** |

**Rule of thumb:** N ≲ 8 → EBS+FSR is fine and cheaper. Wide fan-out → FSx Lustre.
Fixed/rarely-changing reference and a fixed demo → baked AMI.

## Consequences / upstream trail

- nf-spawn#62 (`ext.az`) — released 0.7.0.
- nf-spawn#67 (`ext.fsx` forwarding + mount in #55 staging) — released 0.8.0.
- spawn#206/#208 (`--fsx-create` AZ handling, PERSISTENT_2 offering) — fixed 0.59.0.
- DRA scope gotcha: `spawn --fsx-create` scopes the DRA to the **bucket root**, not
  `--fsx-import-path`. Delete and recreate the DRA scoped to the reference prefix,
  or it imports the whole bucket.
