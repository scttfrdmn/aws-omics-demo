#!/usr/bin/env python3
"""
build_fsx_db.py — stage the human REFERENCE GENOME onto FSx FROM ITS CANONICAL
SOURCE, timing the staging as a measured one-time cost.

The benchmark measures data copy/staging. So the reference is NOT laundered through
old EBS snapshots — it is fetched from its authoritative upstream onto a staging
instance, into s3://BUCKET/reference-fsx/, which an S3-backed FSx for Lustre then
imports. Every CALL_VARIANTS task mounts that one shared FS read-only (no per-volume
FSR credit limit — the reason EBS-snapshot volumes don't scale past ~10 concurrent
readers).

Canonical source (provenance, not a copy of a copy):
  - human_g1k_v37 reference : s3://1000genomes/technical/reference/human_g1k_v37.fasta.gz
                              (the public 1000 Genomes bucket) → gunzip + samtools faidx.

Each phase is TIMED (download_s, gunzip_s, faidx_s, sync_s, bytes) and the timings
written to s3://BUCKET/reference-fsx/staging_timings.json — the measured
"reference staging from source" cost, reported alongside the per-run numbers.

This is a ONE-TIME, amortized setup. The per-run/per-task path stays zero-copy:
tasks read the reference in place off /fsx (the shared Lustre mount).

  Stage 1 (this script's user-data): a c7g staging instance fetches the reference
    from source, times each phase, syncs to s3://BUCKET/reference-fsx/, writes
    timings, exits.
  Stage 2 (manual): spawn launch ... --fsx-create --fsx-s3-bucket BUCKET
    --fsx-import-path s3://BUCKET/reference-fsx ... → capture fs-id → set FSX_ID.

⚠ GATED: --plan prints the plan and the staging user-data; it takes NO action.
  Running the staging incurs real EC2 + S3 spend; launch the printed command
  yourself so the cost is explicitly approved.
"""

from __future__ import annotations

import argparse
import importlib.util
import sys

if importlib.util.find_spec("config") is None:
    sys.exit("config.py not found")
import config as cfg  # type: ignore[import]

S3_PREFIX = f"s3://{cfg.BUCKET}/reference-fsx"
REFERENCE_SRC = "s3://1000genomes/technical/reference/human_g1k_v37.fasta.gz"
REFERENCE_NAME = "human_g1k_v37.fasta"
# samtools comes from the container; pin the same arm64 image the pipeline uses.
SAMTOOLS_IMAGE = "quay.io/aarchbio/samtools:1.23.1--hc7977f4_0"

# Staging user-data: fetch the reference from its canonical source, time every
# phase, faidx, sync to S3, emit timings. Placeholders substituted in
# build_userdata().
_STAGING_USERDATA = r"""#!/bin/bash
set -euxo pipefail
exec > /var/log/fsx-stage.log 2>&1
echo "=== FSx reference staging from canonical source: $(date) ==="
REGION="@@REGION@@"
S3_PREFIX="@@S3_PREFIX@@"
REFERENCE_SRC="@@REFERENCE_SRC@@"
REFERENCE_NAME="@@REFERENCE_NAME@@"
SAMTOOLS_IMAGE="@@SAMTOOLS_IMAGE@@"
WORK=/mnt/stage
mkdir -p "$WORK/reference"
# Stock AL2023 has no Docker; samtools faidx runs via the container. Install
# + start Docker before the faidx phase (download + gunzip are pure aws-cli/gzip).
dnf install -y docker >/dev/null 2>&1
systemctl enable --now docker
docker --version
T() { date +%s.%N; }
JQADD() { python3 -c "import json,sys; d=json.load(open('/tmp/timings.json')) if __import__('os').path.exists('/tmp/timings.json') else {}; d[sys.argv[1]]=float(sys.argv[2]); json.dump(d,open('/tmp/timings.json','w'))" "$1" "$2"; }

# ── Reference: download the gzipped fasta from 1000genomes (public) ──────────
t0=$(T)
aws s3 cp "$REFERENCE_SRC" "$WORK/reference/$REFERENCE_NAME.gz" --no-sign-request --region "$REGION" --no-progress
t1=$(T); JQADD reference_download_s "$(awk -v a=$t0 -v b=$t1 'BEGIN{print b-a}')"
GZ_BYTES=$(stat -c%s "$WORK/reference/$REFERENCE_NAME.gz"); JQADD reference_gz_bytes "$GZ_BYTES"

# ── gunzip to the plain fasta ────────────────────────────────────────────────
t2=$(T)
gunzip -f "$WORK/reference/$REFERENCE_NAME.gz"
t3=$(T); JQADD reference_gunzip_s "$(awk -v a=$t2 -v b=$t3 'BEGIN{print b-a}')"
FA_BYTES=$(stat -c%s "$WORK/reference/$REFERENCE_NAME"); JQADD reference_fasta_bytes "$FA_BYTES"

# ── samtools faidx → the .fai index every CALL_VARIANTS task reads alongside ─
chmod 0777 "$WORK/reference"
t4=$(T)
docker run --rm --user root -v "$WORK/reference:/ref" "$SAMTOOLS_IMAGE" \
    samtools faidx "/ref/$REFERENCE_NAME"
t5=$(T); JQADD reference_faidx_s "$(awk -v a=$t4 -v b=$t5 'BEGIN{print b-a}')"

# ── sync the fasta + .fai to S3 (FSx imports this prefix) ────────────────────
t6=$(T)
aws s3 sync "$WORK/reference/" "$S3_PREFIX/" --region "$REGION" --no-progress --delete
t7=$(T); JQADD reference_sync_s "$(awk -v a=$t6 -v b=$t7 'BEGIN{print b-a}')"

# ── provenance + emit timings ────────────────────────────────────────────────
python3 -c "import json; d=json.load(open('/tmp/timings.json')); d['reference_source']='$REFERENCE_SRC'; d['reference_name']='$REFERENCE_NAME'; d['staged_at']='$(date -u +%Y-%m-%dT%H:%M:%SZ)'; json.dump(d,open('/tmp/timings.json','w'),indent=2)"
aws s3 cp /tmp/timings.json "$S3_PREFIX/staging_timings.json" --region "$REGION" --no-progress
echo "--- staged file inventory ---"
aws s3 ls "$S3_PREFIX/" --region "$REGION"
cat /tmp/timings.json
touch /tmp/SPAWN_COMPLETE
echo "=== staging complete: $(date) ==="
"""


def build_userdata() -> str:
    return (
        _STAGING_USERDATA.replace("@@REGION@@", cfg.REGION)
        .replace("@@S3_PREFIX@@", S3_PREFIX)
        .replace("@@REFERENCE_SRC@@", REFERENCE_SRC)
        .replace("@@REFERENCE_NAME@@", REFERENCE_NAME)
        .replace("@@SAMTOOLS_IMAGE@@", SAMTOOLS_IMAGE)
    )


def plan() -> None:
    import tempfile

    ud = build_userdata()
    path = tempfile.mktemp(suffix="-fsx-stage.sh")
    with open(path, "w") as f:
        f.write(ud)
    print("=== build_fsx_db.py — PLAN (no actions taken) ===\n")
    print(f"Region/Bucket: {cfg.REGION} / {cfg.BUCKET}")
    print(f"FSx S3 import target: {S3_PREFIX}/\n")
    print("Reference staging FROM CANONICAL SOURCE (timed):")
    print(f"  reference <- {REFERENCE_SRC}  (download + gunzip + samtools faidx)")
    print(f"\nStaging user-data written to: {path}")
    print("\nStage 1 — launch the staging instance (needs Docker for samtools faidx):")
    print(f"  spawn launch fsx-stage --instance-type c7g.2xlarge --region {cfg.REGION} \\")
    print("      --az us-east-1a --volume-size 50 \\")
    print(f"      --user-data-file {path} --ttl 1h --wait-for-ssh -y")
    print("  (c7g.2xlarge + 50GB root: room for the gzipped + unzipped ~3GB fasta")
    print("   and its .fai before syncing to S3.)")
    print("\nStage 2 — after staging completes, create the FSx FS:")
    print(
        f"  spawn launch fsx-host --instance-type c7g.large --region {cfg.REGION} --az us-east-1a \\"
    )
    print("      --fsx-create --fsx-lifecycle durable --fsx-ttl 1d \\")
    print(f"      --fsx-s3-bucket {cfg.BUCKET} --fsx-import-path {S3_PREFIX} \\")
    print("      --fsx-storage-capacity 1200 --fsx-throughput 250 --fsx-mount-point /fsx ...")
    print("  Then scope the DRA to /fsx <- reference-fsx, set FSX_ID = '<fs-id>'.")
    print(f"\nMeasured staging cost lands at: {S3_PREFIX}/staging_timings.json")
    print("\n⚠ Running these incurs real EC2 + S3 + FSx spend. Launch them yourself.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--plan", action="store_true", help="print the staging plan + user-data (default)"
    )
    ap.add_argument(
        "--print-userdata", action="store_true", help="print only the staging user-data script"
    )
    args = ap.parse_args()
    if args.print_userdata:
        print(build_userdata())
    else:
        plan()
