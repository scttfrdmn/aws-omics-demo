> ⚠️ **Partly superseded.** This captures the EBS+FSR-era AZ/FSR decision (the
> `ext.az` + FSR-in-one-AZ choice). It pre-dates the FSx Lustre pivot that the
> wide-fan-out FSR credit cliff forced. The current, complete DB-delivery decision
> — including FSx — is [`../docs/decisions/0001-db-delivery-ami-ebs-fsx.md`](../docs/decisions/0001-db-delivery-ami-ebs-fsx.md).
> Kept for the AZ-pinning rationale and the cost comparison of the FSR options.

# Getting fast, fair reference-genome delivery on both benchmark legs — the AZ/FSR decision

The benchmark needs the human reference genome (~3 GB fasta + .fai) on each task
instance **fast and identically on both legs**, so the measured time is
*architecture*, not reference-load. The EBS-volume reference path lazy-loads from
S3 at ~6-8 MB/s unless the snapshot has **Fast Snapshot Restore (FSR)** enabled —
and FSR is **per-AZ**, while nf-spawn (≤0.6.0) doesn't let a task pin its AZ, so a
task can land in a cold AZ and crawl. Three ways to fix it, in time + $:

| | Option A — FSR in all 6 AZs | Option B — bake reference into both AMIs | **Option C — ext.az + FSR in 1 AZ (chosen; shipped in nf-spawn 0.7.0)** |
|---|---|---|---|
| **Setup time (one-time)** | 0 (no code) | ~30 min (2 AMI re-bakes) | ~30 min (nf-spawn patch + build + test + host) — **done** |
| **$/hr while running** | **6 AZs** × $0.75 = **$4.50/hr** | $0 (reference on root) | **1 AZ** × $0.75 = **$0.75/hr** |
| **Cost for a ~2 hr A/B window** | **~$9** | ~$0.08 bake compute | **~$1.50** |
| **Warm-up wait** | ~minutes | none | ~minutes |
| **Keeps EBS-volume reference path in the *measured* benchmark?** | yes | **no** — reference-on-root; volume path becomes synthetic-only | yes |
| **Placement** | best-effort (lands in *some* warmed AZ) | n/a (reference on every root) | **deterministic** (pinned) |
| **Side effects** | 6× FSR spend; disable after or it bills forever | every task root carries the reference → bigger AMI snapshots, slower cold boot, larger per-task EBS | none; reusable; closes the upstream gap |
| **Closes the nf-spawn AZ gap?** | no | no (sidesteps it) | **yes** (ext.az, #62) |

## Why specificity wins (Option C)

The brute-force option (A) and the sidestep (B) both *work around* the missing
capability — A by paying 6× to blanket every AZ, B by abandoning the
volume-delivery path the benchmark exists to measure. **The actual missing
primitive is "put this task in this AZ."** Adding `ext.az` to nf-spawn gives that
primitive, and with it the cheapest, most deterministic, and most honest run:

- **6× cheaper** than A ($0.75 vs $4.50/hr) and pinpoints placement instead of
  hoping EC2 lands the task in a warmed zone.
- **Keeps the EBS-volume reference-delivery path in the measured numbers** (unlike
  B), so the benchmark reflects the architecture the demo actually advocates.
- **Reusable + upstream**: every future volume-on-FSR run benefits, and the gap
  that bit us (FSR per-AZ vs unpinnable tasks) is closed for good.

The one-time ~30 min to add `ext.az` is paid once; the per-run savings and the
determinism recur every time. Specificity beats both paying-to-blanket and
giving-up-the-measurement.

## Status — UNBLOCKED (nf-spawn 0.7.0)

Option C's missing primitive — an `ext.az` directive forwarded to `spawn launch
--az` — **shipped in released nf-spawn 0.7.0** (the resolution of nf-spawn#62).
The demo consumes the official release; nothing in nf-spawn is modified here.

Wired up on the demo side:
- **config.py** — `BENCH_AZ = "us-east-1a"` (the AZ FSR is enabled in).
- **nextflow_config.py** — plugin pinned `nf-spawn@0.7.0`; `render()` emits
  `ext.az` on every label and the fallback when `BENCH_AZ` is set (empty → spawn
  default).
- **worker_script.py** — `TARGET_NF_SPAWN_VERSION="0.7.0"`, pulled from the
  official GitHub release at head boot.
- **FSR** — enable on the reference snapshot in us-east-1a only ($0.75/hr) for the
  run window, then disable: `aws ec2 disable-fast-snapshot-restores
  --availability-zones us-east-1a --source-snapshot-ids <reference>`.

The clean A/B legs in `results/` were captured during exploration on a pre-release
build, so re-run them on 0.7.0 to get the reproducible, official-release numbers.

See [[fsr-az-pinning-nf-spawn-gap]] and [[benchmark-timing-on-spawn-executor]].
