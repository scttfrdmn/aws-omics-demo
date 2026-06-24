"""
config.example.py  --  copy to config.py and fill in before running anything.

    cp config.example.py config.py

config.py is git-ignored. Never commit real account IDs or bucket names.
"""

# --- account / region -------------------------------------------------------
REGION = "us-east-1"  # must be us-east-1 (1000genomes bucket is there; no egress cost)
ACCOUNT_ID = "000000000000"  # your 12-digit AWS account ID
BUCKET = "your-omics-demo-bucket"  # S3 bucket for results + work dir

# --- samples ----------------------------------------------------------------
# How many 1000 Genomes samples to use PER super-population (AFR/EUR/EAS).
# Groups are kept balanced so population-differentiation QC isn't confounded by
# uneven group sizes. 10-13/group gives the full story; 3/group is a quick
# rehearsal run. Low-coverage BAMs are read DIRECTLY from the Open Data bucket
# (s3://1000genomes/phase3/data/) at runtime — no staging into your bucket.
SAMPLES_PER_GROUP = 10

# --- benchmark --------------------------------------------------------------
# Architecture of the measured leg: 'arm64' (Graviton, default) or 'x86'.
# The x86↔arm64 comparison uses matched instance pairs (c7g↔c7i, r7g↔r7i).
BENCH_ARCH = "arm64"
# Pin every task to one AZ so FSR-warmed reference-genome volumes are
# fast-restored (FSR is per-AZ). Empty → spawn's default placement.
BENCH_AZ = ""

# --- AMI --------------------------------------------------------------------
# Filled in by build_ami.py after the AMIs are baked. The tools AMI pre-installs:
# Nextflow, samtools/bcftools, the spawn CLI, and the nf-spawn executor plugin.
# (The human reference genome is delivered via FSx/EBS, not baked — see below.)
AMI_ID = ""  # generic / fallback AMI id
AMI_ID_ARM64 = ""  # Graviton (arm64) tools AMI
AMI_ID_X86 = ""  # x86_64 tools AMI
VOLUME_SIZE = 40  # GB — root volume; must be >= AMI root snapshot size

# --- reference genome delivery (FSx → EBS → baked precedence) ---------------
# The shared read-only human reference (human_g1k_v37.fasta + .fai, ~3 GB) that
# every CALL_VARIANTS task reads. Delivery precedence in nextflow_config.py:
#   1. FSx for Lustre  (FSX_ID set)        — wide fan-out, no FSR credit limit
#   2. EBS snapshot    (REFERENCE_SNAPSHOT) — clean at small N
#   3. baked into AMI  (neither set)
#
# FSx (the wide-fan-out answer): one S3-backed shared filesystem, mounted
# read-only by every task. The reference lives at <FSX_MOUNT>/reference.
# NOTE: nf-spawn 0.8.0 does not yet forward ext.fsx (nf-spawn#67) — leave FSX_ID
# empty until that lands; the config is wired ready for it.
FSX_ID = ""  # FSx for Lustre filesystem id (fs-...) or "" to fall back
FSX_MOUNT = "/fsx"  # mount point on each task instance

# EBS-snapshot fallback: a pre-built snapshot of the indexed reference, mounted
# read-only at REFERENCE_MOUNT on the task (nf-spawn ext.volumes).
REFERENCE_SNAPSHOT = ""  # EBS snapshot id (snap-...) or "" to fall back to baked
REFERENCE_MOUNT = "/opt/reference"  # mount point for the reference volume

# --- Nextflow head instance -------------------------------------------------
# A small instance that runs Nextflow + nf-spawn to orchestrate the pipeline.
# Each pipeline task gets its own purpose-sized instance (defined in
# nextflow_config.py by process label).
HEAD_INSTANCE_TYPE = "c7g.large"

# Auto-terminate after this long even if the pipeline hasn't completed.
INSTANCE_TTL = "3h"

# spawn job name — used to identify the running head instance via `spawn list`
JOB_NAME = "omics-demo"

# --- Bedrock / AI insights --------------------------------------------------
# Claude Sonnet synthesizes the variant-calling results into plain-language
# insights (variant counts, Ti/Tv, population differentiation).
BEDROCK_REGION = "us-west-2"
BEDROCK_MODEL = "us.anthropic.claude-sonnet-4-6"

# --- web app ----------------------------------------------------------------
HOST = "127.0.0.1"
PORT = 8000
