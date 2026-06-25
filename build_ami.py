#!/usr/bin/env python3
"""
build_ami.py  --  bake an Amazon Machine Image (AMI) with everything the
                  demo pipeline needs pre-installed.

Run this ONCE before the talk.  It takes ~30-45 minutes.  The resulting AMI
eliminates all software download/install time on demo day -- the instances
boot ready to run Nextflow immediately.

What the AMI contains (a lean TOOLS image — no reference genome):
  - Amazon Linux 2023 (ARM64 / Graviton3, or x86_64 for the x86 leg)
  - Nextflow 24.x  (the workflow engine)
  - Docker + AWS CLI  (nf-spawn runs tasks via `docker run` on the host;
    bcftools/samtools come from the biocontainers image at runtime — not baked)
  - nf-spawn plugin + nf-amazon plugin (pre-cached for the s3:// workDir)
  - spawn CLI + spored agent  (Spawn's termination daemon)

The human reference genome is NOT baked in — it rides a shared FSx for Lustre
filesystem (or, at small N, an attached EBS volume), mounted read-only by every
CALL_VARIANTS task via nf-spawn ext.fsx / ext.volumes:
  - human_g1k_v37.fasta (~3 GB) + .fai → staged once from
    s3://1000genomes/technical/reference/human_g1k_v37.fasta.gz onto FSx
    (gunzip → samtools faidx). Every per-sample task reads it in place; the
    s3:// db_path marker basename ('reference') is symlinked to the mount
    (nf-spawn #55), so there is zero per-task copy.
  This keeps the AMI small, lets task root volumes stay small, and makes a
  reference update a re-stage instead of a full AMI rebake. See
  docs/ami-vs-data-volume.md.

Re-running safely:
  If AMI_ID in config.py is already set, this script prints the existing
  AMI details and exits without creating a duplicate.

Requires:
  - spawn CLI installed (brew install spore-host/tap/spawn)
  - AWS credentials with EC2 + IAM permissions
  - ~$1-2 in EC2 costs for the bake instance (auto-terminates when done)
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
import time

import boto3

# The bake script that runs INSIDE the instance to install everything.
# Passed to spawn launch via --user-data.
_BAKE_SCRIPT = """#!/bin/bash
set -euxo pipefail
# Log everything so we can see what happened if the AMI bake fails.
exec > /var/log/ami-bake.log 2>&1

echo "=== 1000 Genomes Variant Calling Demo AMI Bake ==="
echo "Started: $(date)"

# Upload log to S3 on EXIT (success or failure) so we can diagnose failures
# even after the instance terminates.  Bucket must exist before bake starts.
BAKE_BUCKET="scttfrdmn-omics-demo"
BAKE_REGION="us-east-1"
upload_log() {
    aws s3 cp /var/log/ami-bake.log \\
        "s3://${BAKE_BUCKET}/bake-logs/ami-bake-$(date +%Y%m%d-%H%M%S).log" \\
        --region "${BAKE_REGION}" 2>/dev/null || true
}
trap upload_log EXIT

# --- System packages --------------------------------------------------------
dnf update -y
dnf install -y \\
    java-21-amazon-corretto \\
    docker \\
    git \\
    wget \\
    parallel \\
    htop \\
    squashfs-tools \\
    fuse \\
    fuse-libs

# Start Docker (required for the bcftools/samtools biocontainers)
systemctl enable --now docker
usermod -aG docker ec2-user

# Note: Singularity/Apptainer has no pre-built ARM64/aarch64 RPM.
# We use Docker instead — the variant-calling pipeline runs bcftools/samtools
# inside Docker, and Docker is the standard container runtime on AL2023.
docker --version

# Note: bcftools and samtools are NOT installed on the host. The CALL_VARIANTS /
# MERGE_VCFS / VCF_STATS processes run them inside a Docker container
# (quay.io/biocontainers/bcftools + samtools, set in nextflow.config), pulled at
# pipeline runtime. arm64 availability must be verified — genomics biocontainers
# may need arm64 rebuilds; neither arch emulates the other.

# --- Nextflow ---------------------------------------------------------------
mkdir -p /usr/local/bin
cd /usr/local/bin
wget -q --timeout=120 https://get.nextflow.io -O nextflow
chmod +x nextflow
./nextflow self-update  # pull latest stable version

# --- Python packages --------------------------------------------------------
# boto3 is needed by the head node script for S3 progress reporting.
dnf install -y python3-pip
python3 -m pip install --quiet boto3

# Note: the bcftools/samtools Docker image is NOT pre-pulled here.
# Nextflow pulls it automatically on first use from the biocontainers registry.
# Pre-pulling would require `docker login` which we avoid on bake instances.
echo "Docker ready — bcftools/samtools container will be pulled at pipeline runtime"

# --- Reference genome: NOT baked into the AMI --------------------------------
# The human_g1k_v37 reference (~3 GB fasta + .fai) now lives on a SHARED FSx for
# Lustre filesystem (or, at small N, an EBS snapshot volume), mounted read-only
# on each CALL_VARIANTS task at the FSx mount via nf-spawn ext.fsx / ext.volumes
# (config.FSX_ID / config.REFERENCE_SNAPSHOT). Stage it once with:
#   aws s3 cp s3://1000genomes/technical/reference/human_g1k_v37.fasta.gz - \
#     | gunzip > human_g1k_v37.fasta && samtools faidx human_g1k_v37.fasta
# then copy onto the FSx (or build an EBS snapshot from it). This keeps the AMI a
# lean tools image: the reference is a re-stageable filesystem, task root volumes
# stay small, and a reference update is a re-stage rather than a full AMI rebake.
# See docs/ami-vs-data-volume.md.
mkdir -p /fsx  # default FSx mount point for the shared reference on task nodes

# --- spawn CLI --------------------------------------------------------------
# Install from spore-host/spawn GitHub releases (ARM64 RPM for AL2023).
# Releases live at github.com/spore-host/spawn, not spore-host/spore-host.
SPAWN_VER=$(curl -sf https://api.github.com/repos/spore-host/spawn/releases/latest \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['tag_name'].lstrip('v'))" 2>/dev/null \
    || echo "0.36.6")
# Arch-aware spawn rpm: an arm64 AMI needs the aarch64 binary (spawn is a Go
# binary, not arch-portable).  x86 bakes still get amd64.
case "$(uname -m)" in
    aarch64) SPAWN_RPM_ARCH="arm64" ;;
    x86_64)  SPAWN_RPM_ARCH="amd64" ;;
    *)       echo "Unknown arch $(uname -m)"; exit 1 ;;
esac
curl -fsSL --output /tmp/spawn.rpm \\
    "https://github.com/spore-host/spawn/releases/download/v${SPAWN_VER}/spawn_${SPAWN_VER}_linux_${SPAWN_RPM_ARCH}.rpm"
dnf install -y /tmp/spawn.rpm
spawn version

# --- nf-spawn plugin (Nextflow executor for spawn) --------------------------
# Download the pre-built release ZIP — no Gradle build needed.
# 0.8.0 adds ext.fsx/ext.efs (→ --fsx-id/--efs-id) so every CALL_VARIANTS task
# can read the shared reference genome off one FSx filesystem. The head node
# bootstrap re-pins this same version, so a stale cache is harmless.
NF_SPAWN_VERSION="0.8.0"
NF_PLUGIN_DIR=/opt/nextflow_cache/plugins
dnf install -y unzip
PLUGIN_DEST="${NF_PLUGIN_DIR}/nf-spawn-${NF_SPAWN_VERSION}"
mkdir -p "${PLUGIN_DEST}"
curl -fsSL "https://github.com/spore-host/nf-spawn/releases/download/v${NF_SPAWN_VERSION}/nf-spawn-${NF_SPAWN_VERSION}.zip" \
    -o /tmp/nf-spawn.zip
unzip -q /tmp/nf-spawn.zip -d "${PLUGIN_DEST}"
rm /tmp/nf-spawn.zip
echo "nf-spawn installed (exploded into classes/): ${PLUGIN_DEST}"
find "${PLUGIN_DEST}" -type f

# --- nf-amazon plugin (required for s3:// workDir) -------------------------
# Nextflow downloads plugins on first use; pre-cache it now so demo runs
# don't need internet access or suffer a cold-start delay.
# nf-amazon provides the S3 FileSystem implementation that lets Nextflow
# use s3://bucket/work/ as the work directory between task instances.
mkdir -p /opt/nextflow_cache
NXF_HOME=/opt/nextflow_cache \\
    /usr/local/bin/nextflow plugin install nf-amazon@2.8.0

# --- Custom pipeline: shipped at runtime, not baked -------------------------
# The variant-calling main.nf (CALL_VARIANTS → MERGE_VCFS → VCF_STATS) is a
# custom, self-contained pipeline the head node uploads to S3 and runs with
# `nextflow run main.nf` — there is no nf-core pipeline to `nextflow pull` here.
echo "Custom main.nf is shipped by the head node at runtime — nothing to pull."

# --- Permissions ------------------------------------------------------------
# Make everything readable by all users (pipeline runs as ec2-user)
chmod -R 755 /opt/nextflow_cache

# --- Completion signal ------------------------------------------------------
echo "=== AMI bake complete: $(date) ==="
touch /tmp/SPAWN_COMPLETE
"""


def _spawn_json(args: list[str]) -> dict:
    """Run a spawn command with -o json and return the parsed output."""
    import json

    result = subprocess.run(
        ["spawn"] + args + ["-o", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        print(f"spawn error: {result.stderr[:500]}", file=sys.stderr)
        sys.exit(1)
    return json.loads(result.stdout)


def bake_ami(cfg) -> str:
    """Launch a bake instance, install everything, create the AMI.

    Returns the new AMI ID (also printed for pasting into config.py).
    """
    import json

    from botocore.exceptions import ClientError

    # Ensure the S3 bucket exists — the bake script uploads its log there on exit.
    s3 = boto3.client("s3", region_name=cfg.REGION)
    try:
        s3.create_bucket(Bucket=cfg.BUCKET)
        print(f"  Created bucket: s3://{cfg.BUCKET}")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise
        print(f"  Bucket exists: s3://{cfg.BUCKET}")

    # Bake instance arch follows cfg.BENCH_ARCH so the AMI (and its baked spawn
    # CLI binary) target the right family. The reference genome is NOT downloaded
    # here (it rides FSx now), so the bake is light — a small instance is plenty
    # for dnf installs + plugin caching. spawn auto-detects the latest AL2023 base
    # for the instance arch (arm64 type → aarch64 base + arm64 spawn rpm; x86 type
    # → amd64 base + amd64 rpm). Neither arch emulates the other.
    arch = getattr(cfg, "BENCH_ARCH", "arm64")
    bake_instance_type = "c7i.xlarge" if arch == "x86" else "c7g.xlarge"
    bake_name = f"omics-bake-{arch}"

    print("Launching bake instance via spawn...")
    print(f"  Instance type: {bake_instance_type}")
    print(f"  Region: {cfg.REGION}")

    # Write the bake script to a temp file
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as f:
        f.write(_BAKE_SCRIPT)
        bake_script_path = f.name

    # Launch via spawn: ARM64 AL2023, 20 GB EBS (requires spawn >= 0.36.3).
    # 20 GB is sufficient: a lean tools image (Nextflow + Docker + plugins) with
    # no reference genome baked in — the reference rides FSx.
    # Notes:
    #   - --ami omitted: spawn auto-detects latest AL2023 for the region/arch
    #   - -o json omitted: spawn's TUI overrides it and outputs ANSI progress,
    #     not JSON; we use `spawn list -o json` after launch to find the instance
    subprocess.run(
        [
            "spawn",
            "launch",
            bake_name,
            "--instance-type",
            bake_instance_type,
            "--region",
            cfg.REGION,
            "--volume-size",
            "20",  # lean tools AMI — no reference genome baked in (rides FSx)
            "--user-data-file",
            bake_script_path,
            "--ttl",
            "2h",  # lean bake: dnf installs + plugin caching, no large DB download
            "--wait-for-ssh",
            "-y",
        ],
        check=False,
    )

    # Look up the instance by name via `spawn list -o json`.
    print("  Looking up instance ID via spawn list...")
    instance_id = None
    for _ in range(10):
        list_result = subprocess.run(
            ["spawn", "list", "-o", "json"],
            capture_output=True,
            text=True,
            check=False,
        )
        try:
            instances = json.loads(list_result.stdout)
            if not isinstance(instances, list):
                instances = [instances]
            for inst in instances:
                if inst.get("name") == bake_name:
                    instance_id = inst.get("instance_id") or inst.get("InstanceId")
                    break
        except (json.JSONDecodeError, KeyError):
            pass
        if instance_id:
            break
        time.sleep(5)

    if not instance_id:
        print("  Could not find omics-bake instance via spawn list.")
        sys.exit(1)
    print(f"\n  Instance launched: {instance_id}")
    print("  Installing Nextflow, nf-spawn, Docker, spawn CLI...")
    print("  This takes ~30-45 minutes.  Grab a coffee.\n")

    # Wait for the bake script to signal completion via SPAWN_COMPLETE,
    # then snapshot it into an AMI using `spawn ami create`.
    print("  Waiting for bake to complete (polling every 60s)...")
    for attempt in range(180):
        time.sleep(60)
        status_result = subprocess.run(
            ["spawn", "status", bake_name, "--check-complete"],
            capture_output=True,
            check=False,
        )
        if status_result.returncode == 0:
            print(f"  Bake complete after ~{attempt + 1} minutes")
            break
        if status_result.returncode == 1:
            print("  Bake FAILED — check s3://bucket/bake-logs/ for details")
            sys.exit(1)
        if (attempt + 1) % 5 == 0:
            print(f"  Still running... ({attempt + 1} min elapsed)")
    else:
        print("  Bake timed out after 180 minutes")
        sys.exit(1)

    ami_name = f"nf-spawn-{arch}-tools-v0.8.0-omics"
    print(f"\n  Creating AMI '{ami_name}' via spawn ami create...")
    result = subprocess.run(
        [
            "spawn",
            "ami",
            "create",
            bake_name,
            "--name",
            ami_name,
            "--description",
            f"Omics demo (1000 Genomes variant calling) {arch} tools AMI: Nextflow + nf-spawn 0.8.0 + Docker (reference genome on FSx, not baked)",
            "--wait",
            "-o",
            "json",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    try:
        data = json.loads(result.stdout)
        ami_id = data.get("image_id") or data.get("ImageId") or data.get("ami_id")
    except (json.JSONDecodeError, AttributeError):
        # Fall back to parsing stdout for the AMI ID
        import re

        m = re.search(r"ami-[0-9a-f]+", result.stdout + result.stderr)
        ami_id = m.group(0) if m else None

    if not ami_id:
        print(f"  Could not parse AMI ID from spawn output:\n{result.stdout}\n{result.stderr}")
        sys.exit(1)

    print(f"  AMI ready: {ami_id}")
    subprocess.run(["spawn", "terminate", bake_name, "-y"], check=False)
    return ami_id


if __name__ == "__main__":
    if importlib.util.find_spec("config") is None:
        sys.exit("config.py not found — copy config.example.py and fill it in.")

    import config as cfg  # type: ignore[import]

    # The bake produces a lean native TOOLS AMI (Nextflow + Docker + nf-spawn +
    # spawn CLI) for the arch in cfg.BENCH_ARCH. The reference genome is NOT baked
    # — it rides FSx — and bcftools/samtools run in containers, so the whole
    # variant-calling pipeline runs native (no emulation). Bake each arch by
    # setting BENCH_ARCH ('arm64' or 'x86') in config.py and re-running.
    arch = getattr(cfg, "BENCH_ARCH", "arm64")
    ami_var = "AMI_ID_X86" if arch == "x86" else "AMI_ID_ARM64"
    if getattr(cfg, ami_var, ""):
        print(f"{arch} AMI already configured: {getattr(cfg, ami_var)}")
        print(f"To rebuild: set {ami_var} = '' in config.py and re-run.")
        sys.exit(0)

    print(f"=== 1000 Genomes Variant Calling Demo — {arch} AMI Build ===\n")
    ami_id = bake_ami(cfg)

    print("\nDone.  Paste into config.py:")
    print(f'  {ami_var} = "{ami_id}"')
    print("\nNext step: make demo")
