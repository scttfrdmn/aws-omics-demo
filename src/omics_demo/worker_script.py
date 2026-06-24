"""
worker_script.py  --  generate the cloud-init bash script for the Nextflow head node.

Architecture:
  app.py launches ONE small head instance (t4g.small) via spawn.
  The head instance runs Nextflow with the nf-spawn executor plugin.
  Nextflow dispatches each pipeline task to its own ephemeral EC2 instance
  (sized per process label), which self-terminates when the task completes.

Data flow:
  1000 Genomes (s3://1000genomes/phase3/data/) ──► CALL_VARIANTS task instances
                            (low-coverage BAM, read directly — pre-aligned)
                                        │ per-sample VCF.gz
                                        ▼ s3://bucket/work/
                                   MERGE_VCFS instance
                                        │ cohort merged.vcf.gz
                                        ▼ s3://bucket/work/
                                   VCF_STATS instance
                                        ▼ stats.json
                               s3://bucket/results/

The CALL_VARIANTS fan-out reads the SHARED, read-only human reference genome
(human_g1k_v37.fasta + .fai) from FSx — the analog of the Kraken2/MetaPhlAn DB
in the microbiome demo. There is no SRA fetch stage: the BAMs are already
aligned, so each task reads its BAM straight from s3://1000genomes.

The head instance is responsible for:
  1. Writing nextflow.config (uploaded from app.py as an S3 object)
  2. Building the SAMPLES csv from the sample list
     (sample_id,population,super_population,bam_path)
  3. Running `nextflow run main.nf` (the custom variant-calling pipeline)
  4. Writing a progress.json to S3 periodically (dashboard polls this)
  5. Writing final summary.json on completion
  6. Touching /tmp/SPAWN_COMPLETE so spawn knows the head is done

Data volume tracking:
  CALL_VARIANTS reports bytes read from s3://1000genomes (the BAM) and the
  reference; we capture this and the output VCF sizes to show the per-sample
  data movement in the dashboard.
"""

from __future__ import annotations

import tempfile

_TOK_BUCKET = "@@BUCKET@@"
_TOK_REGION = "@@REGION@@"
_TOK_JOB_NAME = "@@JOB_NAME@@"
_TOK_NF_CFG = "@@NF_CONFIG_KEY@@"  # S3 key for the rendered nextflow.config
_TOK_SRR_KEY = "@@SRR_LIST_KEY@@"  # S3 key for the sample list JSON
_TOK_MAIN_NF = "@@MAIN_NF_KEY@@"  # S3 key for the custom main.nf pipeline
# db_path value for the shared reference genome — an s3:// marker URI (NOT a
# head-local mount path). An s3:// db_path is the same filesystem as the s3://
# workDir, so the head does NO foreign-file copy; the basename ('reference')
# matches the ext.volumes / ext.fsx mount so nf-spawn symlinks the staged input
# to the volume on the task (zero copy). There is ONE marker now: the reference.
_TOK_REFERENCE_DBPATH = "@@REFERENCE_DB_PATH@@"

# ---------------------------------------------------------------------------
# Custom Nextflow pipeline (main.nf)
# ---------------------------------------------------------------------------
# Shipped as a heredoc embedded here (the _MAIN_NF constant) and uploaded to S3
# by upload_main_nf(); the head node downloads it and runs `nextflow run`.
# Chosen over a separate pipeline.nf file because the rest of the head bootstrap
# already ships its companion artifacts (nextflow.config, sample list) the same
# way — one upload path, one S3 prefix, no extra file to keep in sync.
#
# DAG: CALL_VARIANTS ×N (parallel) → MERGE_VCFS ×1 → VCF_STATS ×1
#   CALL_VARIANTS reads its BAM directly from s3://1000genomes and the shared
#   reference from FSx (delivered via the db_path s3:// marker + nf-spawn #55
#   symlink). Region is '20' (human_g1k_v37 contigs are named '20', not 'chr20').
# Each process runs on its own nf-spawn EC2 instance.
_MAIN_NF = r'''// main.nf — germline variant calling on 1000 Genomes low-coverage BAMs.
// CALL_VARIANTS (per sample, parallel) → MERGE_VCFS (cohort) → VCF_STATS (QC).
// nf-spawn executor: one ephemeral EC2 instance per task.
nextflow.enable.dsl = 2

// Shared, read-only human reference genome. db_path is an s3:// MARKER whose
// basename is 'reference'; nf-spawn (#55) symlinks the staged input to the FSx
// mount on the task so the .fasta/.fai are read in place (zero copy).
params.reference = params.reference ?: 'reference'
// Region restriction for demo speed. human_g1k_v37 names the contig '20'
// (NOT 'chr20' — that is the hg19/GRCh38 'chr'-prefixed convention).
params.regions   = params.regions   ?: '20'

process CALL_VARIANTS {
    label 'process_medium'
    tag { sample_id }
    // The reference rides FSx (db_path marker → ext.fsx/ext.volumes symlink).
    // bcftools/samtools come from the container (set in nextflow.config).

    input:
    tuple val(sample_id), val(population), val(super_population), val(bam_path)
    // The reference fasta + index, delivered read-only via the FSx mount.
    tuple path(reference), path(reference_idx)

    output:
    tuple val(sample_id), val(population), val(super_population),
          path("${sample_id}.vcf.gz"), path("${sample_id}.vcf.gz.tbi")

    script:
    """
    set -euxo pipefail

    # ── Environment provenance ───────────────────────────────────────────────
    # Captured once so every timing below is interpretable: a given throughput
    # is only meaningful qualified by instance type / network / placement. All
    # probes are best-effort (|| fallback) so they never fail the sample.
    TOK=\$(curl -sf -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 120" 2>/dev/null || true)
    imds() { curl -sf -H "X-aws-ec2-metadata-token: \${TOK}" "http://169.254.169.254/latest/meta-data/\$1" 2>/dev/null; }
    INSTANCE_TYPE=\$(imds instance-type || echo unknown)
    INSTANCE_ID=\$(imds instance-id || echo unknown)
    AZ=\$(imds placement/availability-zone || echo unknown)
    LIFECYCLE=\$(imds instance-life-cycle || echo unknown)
    VCPUS=\$(nproc 2>/dev/null || echo 0)
    IFACE=\$(ip route show default 2>/dev/null | awk '/default/ {print \$5; exit}')
    [ -n "\$IFACE" ] || IFACE=\$(ls /sys/class/net 2>/dev/null | grep -E '^(en|eth)' | head -1 || echo eth0)
    NET_DRIVER=\$(ethtool -i "\$IFACE" 2>/dev/null | sed -n 's/^driver: //p' || echo unknown)
    UNAME_ARCH=\$(uname -m 2>/dev/null || echo unknown)

    # ── Phase 1: pull the aligned BAM from s3://1000genomes (the staging cost) ─
    # 1000 Genomes is in us-east-1 — same region as these instances, no egress
    # charge. The BAMs are PRE-ALIGNED (no SRA fetch / no realignment needed).
    T0=\$(date +%s.%N)
    aws s3 cp ${bam_path} ./${sample_id}.bam \\
        --no-sign-request --region us-east-1 --no-progress
    T1=\$(date +%s.%N)
    BAM_BYTES=\$(stat -c%s ./${sample_id}.bam 2>/dev/null || echo 0)

    # ── Phase 2: index the BAM if it lacks a .bai ────────────────────────────
    # bcftools mpileup needs a coordinate-sorted, indexed BAM. The 1000G
    # low-coverage BAMs are sorted; their .bai is a sibling object on S3 that we
    # do NOT fetch (we only stage the .bam), so index locally. (Matches the
    # `if [ ! -f .bai ]; then samtools index` guard in the original omics-demo.)
    if [ ! -f ./${sample_id}.bam.bai ]; then
        samtools index ./${sample_id}.bam
    fi
    T2=\$(date +%s.%N)

    # ── Phase 3: call variants on chromosome 20 against the FSx-staged ref ────
    # bcftools mpileup pipes straight into bcftools call -mv (multiallelic +
    # variants-only), bgzipped output. -r ${params.regions} limits to one contig
    # for demo speed. ${reference} is the symlinked FSx reference fasta; its
    # .fai (${reference_idx}) is staged alongside so faidx isn't recomputed.
    bcftools mpileup -f ${reference} -r ${params.regions} ./${sample_id}.bam \\
        | bcftools call -mv -Oz -o ${sample_id}.vcf.gz
    bcftools index -t ${sample_id}.vcf.gz
    T3=\$(date +%s.%N)
    VCF_BYTES=\$(stat -c%s ./${sample_id}.vcf.gz 2>/dev/null || echo 0)
    rm -f ./${sample_id}.bam ./${sample_id}.bam.bai

    # ── Emit per-sample data-movement timings (published to results/staging/) ─
    DL_S=\$(awk -v a=\$T0 -v b=\$T1 'BEGIN{printf "%.3f", b-a}')
    IDX_S=\$(awk -v a=\$T1 -v b=\$T2 'BEGIN{printf "%.3f", b-a}')
    CALL_S=\$(awk -v a=\$T2 -v b=\$T3 'BEGIN{printf "%.3f", b-a}')
    DL_MBPS=\$(awk -v by=\$BAM_BYTES -v s=\$DL_S 'BEGIN{ if(s>0) printf "%.2f",(by/1048576)/s; else printf "0" }')
    cat > ${sample_id}.timings.json <<TIMINGS_EOF
{"sample_id":"${sample_id}","population":"${population}","super_population":"${super_population}","instance_type":"\${INSTANCE_TYPE}","instance_id":"\${INSTANCE_ID}","az":"\${AZ}","lifecycle":"\${LIFECYCLE}","vcpus":\${VCPUS},"net_driver":"\${NET_DRIVER}","arch":"\${UNAME_ARCH}","bam_download_s":\${DL_S},"bam_bytes":\${BAM_BYTES},"bam_mbps":\${DL_MBPS},"samtools_index_s":\${IDX_S},"bcftools_call_s":\${CALL_S},"vcf_gz_bytes":\${VCF_BYTES}}
TIMINGS_EOF
    aws s3 cp ${sample_id}.timings.json ${params.outdir}staging/${sample_id}.timings.json \\
        --region us-east-1 --no-progress || true

    echo "Completed variant calling for sample ${sample_id}"
    """
}

process MERGE_VCFS {
    label 'process_high'
    // Cohort fan-in: merge all per-sample VCFs into one multi-sample VCF.

    input:
    path('vcfs/*')
    path('vcfs_idx/*')

    output:
    tuple path('merged.vcf.gz'), path('merged.vcf.gz.tbi')

    script:
    """
    set -euxo pipefail
    ls vcfs/*.vcf.gz > vcf_list.txt
    bcftools merge -l vcf_list.txt -Oz -o merged.vcf.gz
    bcftools index -t merged.vcf.gz
    echo "Completed merging of all per-sample VCFs"
    """
}

process VCF_STATS {
    label 'process_single'
    // Cohort QC: bcftools stats → a stats.json the dashboard reads.

    input:
    tuple path(vcf), path(vcf_idx)

    output:
    path('stats.txt')
    path('stats.json')

    script:
    """
    set -euxo pipefail
    bcftools stats ${vcf} > stats.txt

    # Pull the population-genetics QC numbers out of the stats text. The 'SN'
    # (summary numbers) lines are tab-separated; the value is the last field, so
    # we key on the trailing label and take \$NF (avoids hard-coding a column or
    # embedding a tab/regex escape). The ts/tv ratio prints on its own SN line.
    SNPS=\$(grep -m1 "number of SNPs:" stats.txt | awk '{print \$NF}')
    INDELS=\$(grep -m1 "number of indels:" stats.txt | awk '{print \$NF}')
    RECORDS=\$(grep -m1 "number of records:" stats.txt | awk '{print \$NF}')
    TSTV=\$(grep -m1 "ts/tv:" stats.txt | awk '{print \$NF}')
    : "\${SNPS:=0}" "\${INDELS:=0}" "\${RECORDS:=0}" "\${TSTV:=0}"

    cat > stats.json <<STATS_EOF
{"total_records": \${RECORDS}, "snps": \${SNPS}, "indels": \${INDELS}, "ti_tv_ratio": \${TSTV}}
STATS_EOF
    echo "Completed statistics calculation"
    """
}

workflow {
    // Parse the SAMPLES csv: sample_id,population,super_population,bam_path
    Channel
        .fromPath(params.samples)
        .splitCsv(header: true)
        .map { row -> [ row.sample_id, row.population, row.super_population, row.bam_path ] }
        .set { samples_ch }

    // The shared reference fasta + .fai (db_path marker → FSx symlink on task).
    // .first() so every CALL_VARIANTS task reads the same singleton reference.
    Channel
        .of( tuple( file("${params.reference}"), file("${params.reference}.fai") ) )
        .first()
        .set { reference_ch }

    // Fan-out: one nf-spawn EC2 instance per sample.
    CALL_VARIANTS(samples_ch, reference_ch)

    // Fan-in: collect all per-sample VCFs + indexes, merge into one cohort VCF.
    CALL_VARIANTS.out.map { it[3] }.collect().set { vcfs_ch }
    CALL_VARIANTS.out.map { it[4] }.collect().set { vcfs_idx_ch }
    MERGE_VCFS(vcfs_ch, vcfs_idx_ch)

    // Cohort QC stats.
    VCF_STATS(MERGE_VCFS.out)
}
'''

_HEAD_SCRIPT = r"""#!/bin/bash
set -euxo pipefail
exec > /var/log/nextflow-head.log 2>&1

echo "=== 1000 Genomes Variant Calling Demo — Head Node ==="
echo "Started: $(date)"
echo "Instance: $(curl -sf http://169.254.169.254/latest/meta-data/instance-id || echo unknown)"

BUCKET="@@BUCKET@@"
REGION="@@REGION@@"
JOB_NAME="@@JOB_NAME@@"
NF_CONFIG_KEY="@@NF_CONFIG_KEY@@"
SRR_LIST_KEY="@@SRR_LIST_KEY@@"
MAIN_NF_KEY="@@MAIN_NF_KEY@@"
RESULTS_PREFIX="s3://${BUCKET}/results/${JOB_NAME}"
PROGRESS_KEY="results/${JOB_NAME}/progress.json"

# ── Ensure PATH includes tool install locations ──────────────────────────────
export PATH="/usr/local/bin:/usr/bin:/bin:${PATH}"
# AWS_DEFAULT_REGION is required by the spawn CLI for STS credential discovery.
# The EC2 instance profile provides credentials; the region must be set explicitly.
export AWS_DEFAULT_REGION="@@REGION@@"
export AWS_REGION="@@REGION@@"
# Force regional STS endpoint to avoid VPC endpoint routing issues.
export AWS_STS_REGIONAL_ENDPOINTS=regional
# HOME is required by spawn CLI to locate its config directory.
# Nextflow tasks run in a context where HOME may not be set.
export HOME=/root
# Ensure root has an SSH key — spawn needs it to connect to launched instances.
# spawn#37: spawn should auto-generate this; workaround until that's fixed.
if [ ! -f /root/.ssh/id_rsa ]; then
    mkdir -p /root/.ssh
    ssh-keygen -t rsa -b 4096 -f /root/.ssh/id_rsa -N '' 2>&1
fi

# ── Install nf-spawn plugin from pre-built release ZIP ───────────────────────
# Releases publish a pre-built ZIP with the correct classes/ structure.
# No build step needed — just download and unzip.
# 0.7.0 adds ext.az (→ --az, pin tasks to the FSR AZ, #62); 0.8.0 adds ext.fsx/
# ext.efs (→ --fsx-id/--efs-id, shared reference filesystem, #67) for wide
# fan-out over a stable reference without the EBS+FSR per-volume credit limit.
TARGET_NF_SPAWN_VERSION="0.8.0"
NF_PLUGIN_DIR="/opt/nextflow_cache/plugins"
NF_SPAWN_PLUGIN_DIR="${NF_PLUGIN_DIR}/nf-spawn-${TARGET_NF_SPAWN_VERSION}"
if [ ! -d "${NF_SPAWN_PLUGIN_DIR}/classes" ]; then
    echo "Installing nf-spawn v${TARGET_NF_SPAWN_VERSION} from release ZIP..."
    ZIP_URL="https://github.com/spore-host/nf-spawn/releases/download/v${TARGET_NF_SPAWN_VERSION}/nf-spawn-${TARGET_NF_SPAWN_VERSION}.zip"
    mkdir -p "${NF_SPAWN_PLUGIN_DIR}"
    curl -fsSL "${ZIP_URL}" -o /tmp/nf-spawn.zip
    unzip -q /tmp/nf-spawn.zip -d "${NF_SPAWN_PLUGIN_DIR}"
    rm /tmp/nf-spawn.zip
    echo "nf-spawn ${TARGET_NF_SPAWN_VERSION} installed."
fi

# ── Upgrade spawn CLI to latest release ──────────────────────────────────────
# Always run the latest spawn so bug fixes are live without a full AMI rebake.
CURRENT_SPAWN_VERSION=$(spawn version 2>/dev/null | awk '/^Version:/{print $2}' || echo "unknown")
LATEST_SPAWN_VERSION=$(curl -fsSL "https://api.github.com/repos/spore-host/spawn/releases/latest" \
    2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['tag_name'].lstrip('v'))" \
    2>/dev/null || echo "")
if [ -n "${LATEST_SPAWN_VERSION}" ] && [ "${CURRENT_SPAWN_VERSION}" != "${LATEST_SPAWN_VERSION}" ]; then
    echo "Upgrading spawn: ${CURRENT_SPAWN_VERSION} → ${LATEST_SPAWN_VERSION}"
    ARCH=$(uname -m)
    case "${ARCH}" in
        aarch64) SPAWN_ARCH="arm64" ;;
        x86_64)  SPAWN_ARCH="amd64" ;;
        *)        echo "Unknown arch ${ARCH}, skipping spawn upgrade"; SPAWN_ARCH="" ;;
    esac
    if [ -n "${SPAWN_ARCH}" ]; then
        SPAWN_URL="https://github.com/spore-host/spawn/releases/download/v${LATEST_SPAWN_VERSION}/spawn_${LATEST_SPAWN_VERSION}_linux_${SPAWN_ARCH}.tar.gz"
        TMP_DIR=$(mktemp -d)
        curl -fsSL "${SPAWN_URL}" -o "${TMP_DIR}/spawn.tar.gz" \
            && tar -xzf "${TMP_DIR}/spawn.tar.gz" -C "${TMP_DIR}" \
            && chmod +x "${TMP_DIR}/spawn" \
            && mv "${TMP_DIR}/spawn" /usr/local/bin/spawn \
            && echo "spawn upgraded to $(spawn version 2>/dev/null | awk '/^Version:/{print $2}')" \
            || echo "spawn upgrade failed — continuing with ${CURRENT_SPAWN_VERSION}"
        rm -rf "${TMP_DIR}"
    fi
else
    echo "spawn ${CURRENT_SPAWN_VERSION} is current"
fi

# ── Prerequisites check ──────────────────────────────────────────────────────
command -v nextflow || { echo "ERROR: nextflow not found at /usr/local/bin/nextflow"; exit 1; }
command -v spawn    || { echo "ERROR: spawn not found on PATH"; exit 1; }
# NOTE: no reference genome check here. The human_g1k_v37 reference is no longer
# baked into the AMI — it rides a shared FSx filesystem (or EBS volume), mounted
# read-only on each CALL_VARIANTS task via nf-spawn ext.fsx/ext.volumes, not
# present on this head node. The head only orchestrates.

# ── Download config, sample list, and the custom pipeline ────────────────────
mkdir -p /tmp/nf-head
aws s3 cp "s3://${BUCKET}/${NF_CONFIG_KEY}" /tmp/nf-head/nextflow.config \
    --region "${REGION}"
aws s3 cp "s3://${BUCKET}/${SRR_LIST_KEY}" /tmp/nf-head/sample_list.json \
    --region "${REGION}"
aws s3 cp "s3://${BUCKET}/${MAIN_NF_KEY}" /tmp/nf-head/main.nf \
    --region "${REGION}"

# ── Build the SAMPLES csv ────────────────────────────────────────────────────
# Columns: sample_id,population,super_population,bam_path
# Each CALL_VARIANTS task reads its BAM DIRECTLY from s3://1000genomes at task
# runtime (the BAMs are pre-aligned; no SRA fetch). The reference rides FSx.
python3 - << 'PYEOF'
import csv, json

with open("/tmp/nf-head/sample_list.json") as f:
    samples = json.load(f)

rows = []
for item in samples:
    rows.append({
        "sample_id":        item["sample_id"],
        "population":       item.get("population", "unknown"),
        "super_population": item.get("super_population", "unknown"),
        "bam_path":         item["bam_path"],
    })

out = "/tmp/nf-head/samples.csv"
with open(out, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["sample_id", "population",
                                      "super_population", "bam_path"])
    w.writeheader()
    w.writerows(rows)

print(f"Samples CSV: {len(rows)} samples → {out}")
PYEOF

# ── Write initial progress ───────────────────────────────────────────────────
python3 - << 'PYEOF'
import json, boto3, time

s3 = boto3.client("s3", region_name="@@REGION@@")
progress = {
    "status":        "running",
    "started_at":    time.time(),
    "queue_size":    0,
    "tasks_total":   0,
    "tasks_running": 0,
    "tasks_done":    0,
    "tasks_failed":  0,
    # Data volume counters (updated as trace.tsv grows)
    "bam_bytes_read":   0,   # bytes read from s3://1000genomes
    "vcf_bytes":        0,   # total per-sample VCF output bytes
    "result_bytes":     0,   # merged VCF / stats output size
}
s3.put_object(
    Bucket="@@BUCKET@@",
    Key="results/@@JOB_NAME@@/progress.json",
    Body=json.dumps(progress),
)
PYEOF

# ── Install tmux (not in AL2023 base; needed for resilient session) ──────────
dnf install -y tmux 2>&1 | grep -E "^(Installing|Complete|Error)" || true

# ── Run the pipeline in a tmux session ───────────────────────────────────────
# tmux means Nextflow survives any SSH disconnect and can be reattached:
#   ssh ec2-user@<ip> -t "tmux attach -t nf"
# The custom main.nf carries the whole DAG (CALL_VARIANTS → MERGE_VCFS →
# VCF_STATS) in one run — a single Nextflow invocation, not two stages.
cat > /tmp/nf-head/run_pipeline.sh << 'PIPEEOF'
#!/bin/bash
set -euo pipefail
cd /tmp/nf-head

echo "=== Variant calling: CALL_VARIANTS → MERGE_VCFS → VCF_STATS ==="
NXF_HOME=/opt/nextflow_cache \
    /usr/local/bin/nextflow run /tmp/nf-head/main.nf \
    --samples /tmp/nf-head/samples.csv \
    --reference @@REFERENCE_DB_PATH@@ \
    --regions 20 \
    --outdir "${RESULTS_PREFIX}/" \
    -c /tmp/nf-head/nextflow.config \
    -w "s3://${BUCKET}/work/${JOB_NAME}/call/"
PIPEEOF
# Substitute shell variables now (the heredoc above deferred them)
sed -i "s|\${BUCKET}|${BUCKET}|g; s|\${JOB_NAME}|${JOB_NAME}|g; s|\${RESULTS_PREFIX}|${RESULTS_PREFIX}|g" \
    /tmp/nf-head/run_pipeline.sh
chmod +x /tmp/nf-head/run_pipeline.sh

# Run inside tmux so the session survives SSH disconnects, but also in the
# background so we can track the PID.
tmux new-session -d -s nf -x 220 -y 50 2>/dev/null || true
tmux send-keys -t nf "/tmp/nf-head/run_pipeline.sh 2>&1 | tee /tmp/nf-head/nextflow.stdout; echo NF_EXIT:\$? > /tmp/nf-head/pipeline.exit" Enter

# Give Nextflow a moment to start, then grab the tmux child PID.
sleep 5
NF_PID=$(pgrep -f "run_pipeline.sh" | head -1 || echo "0")
echo "Pipeline PID: ${NF_PID}"

# ── Progress monitor (runs alongside Nextflow) ───────────────────────────────
# Polls the Nextflow trace file on S3 and the local .nextflow.log every 15s.
# Updates progress.json so the dashboard has live numbers.
cat > /tmp/nf-head/monitor.py << 'MONEOF'
import boto3, json, sys, time, os

s3        = boto3.client("s3", region_name="@@REGION@@")
bucket    = "@@BUCKET@@"
job_name  = "@@JOB_NAME@@"
trace_key = f"results/{job_name}/trace.tsv"
prog_key  = f"results/{job_name}/progress.json"
nf_pid    = int(sys.argv[1])

# Load the population map from the sample list so we can annotate stats later.
with open("/tmp/nf-head/sample_list.json") as f:
    sample_list = json.load(f)
pop_map = {item["sample_id"]: item.get("super_population", "unknown")
           for item in sample_list}


def read_trace():
    # Parse the Nextflow trace TSV and return list of task dicts.
    try:
        resp = s3.get_object(Bucket=bucket, Key=trace_key)
        lines = resp["Body"].read().decode().splitlines()
    except Exception:
        return []
    if len(lines) < 2:
        return []
    header = lines[0].split("\t")
    return [dict(zip(header, line.split("\t"))) for line in lines[1:] if line.strip()]


def parse_rchar(s):
    # Bytes read/written field from trace (plain integer or 'N/A').
    if not s or s in ("-", "N/A", ""):
        return 0
    try:
        return int(s)
    except ValueError:
        return 0


def is_call_task(name):
    # CALL_VARIANTS is the per-sample fan-out process in main.nf.
    return "call_variants" in name.lower()


def read_stats_json():
    # VCF_STATS publishes stats.json (Ti/Tv, SNP/indel counts) to results/.
    try:
        body = s3.get_object(
            Bucket=bucket, Key=f"results/{job_name}/stats.json"
        )["Body"].read()
        return json.loads(body)
    except Exception:
        return None


started_at = time.time()

while True:
    time.sleep(15)

    # Pipeline is running while run_pipeline.sh is alive OR exit file not yet written
    pipeline_done = os.path.exists("/tmp/nf-head/pipeline.exit")
    nf_running    = not pipeline_done and os.path.exists(f"/proc/{nf_pid}")
    tasks         = read_trace()

    running = sum(1 for t in tasks if t.get("status") == "RUNNING")
    done    = sum(1 for t in tasks if t.get("status") == "COMPLETED")
    failed  = sum(1 for t in tasks if t.get("status") in ("FAILED", "ABORTED"))
    total   = len(tasks)

    # Data volumes from trace rchar/wchar fields on the CALL_VARIANTS tasks.
    bam_bytes = sum(
        parse_rchar(t.get("rchar", "0"))
        for t in tasks if is_call_task(t.get("name", ""))
    )
    vcf_bytes = sum(
        parse_rchar(t.get("wchar", "0"))
        for t in tasks if is_call_task(t.get("name", ""))
    )

    progress = {
        "status":          "complete" if pipeline_done else "running",
        "started_at":      started_at,
        "elapsed_seconds": time.time() - started_at,
        "tasks_total":     total,
        "tasks_running":   running,
        "tasks_done":      done,
        "tasks_failed":    failed,
        "bam_bytes_read":  bam_bytes,
        "vcf_bytes":       vcf_bytes,
        "stats":           read_stats_json() or {},
    }

    s3.put_object(Bucket=bucket, Key=prog_key, Body=json.dumps(progress))

    if pipeline_done:
        break

MONEOF

python3 /tmp/nf-head/monitor.py ${NF_PID} &
MONITOR_PID=$!

# Wait for the pipeline exit file (written by run_pipeline.sh wrapper).
# Falls back to waiting on PID if the tmux child is directly trackable.
echo "Waiting for pipeline to complete..."
while [ ! -f /tmp/nf-head/pipeline.exit ]; do
    # Also check if the process is still alive
    if [ "${NF_PID}" != "0" ] && ! kill -0 "${NF_PID}" 2>/dev/null; then
        sleep 10  # give it a moment to write the exit file
        break
    fi
    sleep 15
done
NF_EXIT=$(grep -o '[0-9]*$' /tmp/nf-head/pipeline.exit 2>/dev/null || echo "0")
echo "Pipeline exit code: ${NF_EXIT}"

# Give monitor one final cycle then stop it
sleep 20
kill ${MONITOR_PID} 2>/dev/null || true

# ── Write final summary ──────────────────────────────────────────────────────
python3 - << 'PYEOF'
import boto3, json

s3         = boto3.client("s3", region_name="@@REGION@@")
bucket     = "@@BUCKET@@"
job_name   = "@@JOB_NAME@@"
results_pf = f"results/{job_name}"

# Read final progress
try:
    prog = json.loads(
        s3.get_object(Bucket=bucket, Key=f"{results_pf}/progress.json")["Body"].read()
    )
except Exception:
    prog = {}

# Read the cohort VCF stats (Ti/Tv, SNP/indel counts) published by VCF_STATS.
try:
    stats = json.loads(
        s3.get_object(Bucket=bucket, Key=f"{results_pf}/stats.json")["Body"].read()
    )
except Exception:
    stats = prog.get("stats", {})

# Per-super-population sample counts (from the sample list).
super_pops: dict = {}
try:
    with open("/tmp/nf-head/sample_list.json") as f:
        for item in json.load(f):
            sp = item.get("super_population", "unknown")
            super_pops[sp] = super_pops.get(sp, 0) + 1
except Exception:
    pass

summary = {
    "total_samples":   prog.get("tasks_done", 0),
    "completed":       prog.get("tasks_done", 0),
    "failed":          prog.get("tasks_failed", 0),
    "elapsed_seconds": prog.get("elapsed_seconds", 0),
    "super_populations": super_pops,
    # Variant-calling QC (Ti/Tv ratio, SNP/indel counts) from bcftools stats.
    "vcf_stats":       stats,
    # Data volumes for the dashboard
    "data_volumes": {
        "bam_bytes_read": prog.get("bam_bytes_read", 0),
        "vcf_bytes":      prog.get("vcf_bytes", 0),
    },
}
s3.put_object(
    Bucket=bucket,
    Key=f"{results_pf}/summary.json",
    Body=json.dumps(summary),
)
print(f"Summary written: {summary['total_samples']} samples, "
      f"{summary['data_volumes']['bam_bytes_read']:,} bytes from 1000genomes")
PYEOF

# ── Harvest per-task billed-time signal ──────────────────────────────────────
# On the spawn executor the Nextflow trace realtime/duration columns are
# wrapper-local (sub-second) and useless for per-stage timing. The AUTHORITATIVE
# per-task wall-clock is nf-spawn's own lifecycle log: "Submitting task ... to
# spawn instance 'nf-X'" and "Task ... completed (exit C) on instance 'nf-X'",
# both timestamped. Upload .nextflow.log so the analysis can parse submit→
# complete per task (≈ EC2 billed lifetime).
aws s3 cp /tmp/nf-head/.nextflow.log \
    "${RESULTS_PREFIX}/nextflow.head.log" --region "${REGION}" --no-progress || true

touch /tmp/SPAWN_COMPLETE
echo "=== Head node complete: $(date) (Nextflow exit: ${NF_EXIT}) ==="
"""


def render(cfg, nf_config_key: str, srr_list_key: str, main_nf_key: str = "") -> str:
    """Return the head node bash script with config values substituted."""
    from . import pipeline

    # db_path s3:// marker URI for the shared reference genome. Its basename
    # ('reference') == the ext.fsx/ext.volumes mount basename so nf-spawn
    # symlinks it to the FSx mount on each CALL_VARIANTS task (zero copy).
    # See pipeline.reference_marker_s3_uri / write_reference_marker.
    reference_db_path = pipeline.reference_marker_s3_uri(cfg, "reference")

    def _sub(s: str) -> str:
        return (
            s.replace(_TOK_BUCKET, cfg.BUCKET)
            .replace(_TOK_REGION, cfg.REGION)
            .replace(_TOK_JOB_NAME, cfg.JOB_NAME)
            .replace(_TOK_NF_CFG, nf_config_key)
            .replace(_TOK_SRR_KEY, srr_list_key)
            .replace(_TOK_MAIN_NF, main_nf_key or f"pipeline/{cfg.JOB_NAME}/main.nf")
            .replace(_TOK_REFERENCE_DBPATH, reference_db_path)
        )

    return _sub(_HEAD_SCRIPT)


def upload_main_nf(cfg) -> str:
    """Upload the custom main.nf pipeline to S3 and return its key.

    The pipeline (CALL_VARIANTS → MERGE_VCFS → VCF_STATS) is rendered from
    _MAIN_NF and stored in S3 alongside the nextflow.config and sample list.
    """
    import boto3

    s3 = boto3.client("s3", region_name=cfg.REGION)
    key = f"pipeline/{cfg.JOB_NAME}/main.nf"
    s3.put_object(
        Bucket=cfg.BUCKET,
        Key=key,
        Body=_MAIN_NF.encode(),
    )
    return key


def write_temp(script: str) -> str:
    """Write script to a NamedTemporaryFile and return its path."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as f:
        f.write(script)
        return f.name


def write_srr_slice(cfg, accessions: list[tuple[str, str, str, str]]) -> str:
    """Upload the full sample list as a single JSON to S3.

    With nf-spawn, Nextflow manages parallelism — we give the head node all
    samples and let queueSize control concurrency.

    Each entry is a 4-tuple (sample_id, population, super_population, bam_path)
    describing one 1000 Genomes low-coverage BAM.

    Returns the S3 key.
    """
    import json

    import boto3

    s3 = boto3.client("s3", region_name=cfg.REGION)
    entries = [
        {
            "sample_id": sample_id,
            "population": population,
            "super_population": super_population,
            "bam_path": bam_path,
        }
        for sample_id, population, super_population, bam_path in accessions
    ]
    key = f"slices/{cfg.JOB_NAME}/sample_list.json"
    s3.put_object(
        Bucket=cfg.BUCKET,
        Key=key,
        Body=json.dumps(entries, indent=2).encode(),
    )
    return key


def upload_nextflow_config(cfg, nf_config_str: str) -> str:
    """Upload the rendered nextflow.config to S3 and return its key."""
    import boto3

    s3 = boto3.client("s3", region_name=cfg.REGION)
    key = f"config/{cfg.JOB_NAME}/nextflow.config"
    s3.put_object(
        Bucket=cfg.BUCKET,
        Key=key,
        Body=nf_config_str.encode(),
    )
    return key
