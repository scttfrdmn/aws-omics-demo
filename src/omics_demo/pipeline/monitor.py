#!/usr/bin/env python3
"""
monitor.py — head-node progress reporter for the variant-calling pipeline.

Runs on the Nextflow head instance alongside the pipeline. Every 15s it parses
the Nextflow trace TSV (on S3) plus the local .nextflow.log and writes a
progress.json the dashboard polls. Exits when the pipeline writes its exit file.

Placeholders (@@TOKEN@@) are substituted by omics_demo.worker_script.render()
before this file is shipped to the head node. Invoked as:
    python3 monitor.py <nextflow_pid>
"""

import json
import os
import sys
import time

import boto3

s3 = boto3.client("s3", region_name="@@REGION@@")
bucket = "@@BUCKET@@"
job_name = "@@JOB_NAME@@"
trace_key = f"results/{job_name}/trace.tsv"
prog_key = f"results/{job_name}/progress.json"
nf_pid = int(sys.argv[1])

# On-demand $/hr for the cost meter, injected by worker_script.render() from
# pricing.instance_rates(). HEAD_RATE = this head's instance; TASK_RATE = the
# CALL_VARIANTS (process_medium) instance — the fan-out term that dominates cost.
HEAD_RATE_USD_HR = float("@@HEAD_RATE@@")
TASK_RATE_USD_HR = float("@@TASK_RATE@@")

# Load the population map from the sample list so we can annotate stats later.
with open("/tmp/nf-head/sample_list.json") as f:
    sample_list = json.load(f)
pop_map = {item["sample_id"]: item.get("super_population", "unknown") for item in sample_list}


def read_trace():
    # Parse the Nextflow trace TSV and return a list of task dicts.
    try:
        resp = s3.get_object(Bucket=bucket, Key=trace_key)
        lines = resp["Body"].read().decode().splitlines()
    except Exception:
        return []
    if len(lines) < 2:
        return []
    header = lines[0].split("\t")
    # zip to the shorter (trace rows may have fewer fields than the header). NB:
    # no strict= kwarg — the head-node AMI runs Python 3.9, where zip() predates it.
    return [dict(zip(header, line.split("\t"))) for line in lines[1:] if line.strip()]  # noqa: B905


def parse_rchar(s):
    # Bytes read/written field from the trace (plain integer or 'N/A').
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
        body = s3.get_object(Bucket=bucket, Key=f"results/{job_name}/stats.json")["Body"].read()
        return json.loads(body)
    except Exception:
        return None


started_at = time.time()
last_tick = started_at
cost_usd = 0.0  # accumulated, monotonic — a Riemann sum of the burn rate

while True:
    time.sleep(15)

    # Pipeline is running while run_pipeline.sh is alive OR the exit file is unwritten.
    pipeline_done = os.path.exists("/tmp/nf-head/pipeline.exit")
    nf_running = not pipeline_done and os.path.exists(f"/proc/{nf_pid}")
    tasks = read_trace()

    running = sum(1 for t in tasks if t.get("status") == "RUNNING")
    done = sum(1 for t in tasks if t.get("status") == "COMPLETED")
    failed = sum(1 for t in tasks if t.get("status") in ("FAILED", "ABORTED"))
    total = len(tasks)

    # ── Cost: integrate the burn rate over this poll interval ────────────────
    # At each tick, the instantaneous burn is the head + every currently-RUNNING
    # task instance. Accumulating (rate × dt) over the actual intervals gives the
    # real billed instance-seconds — monotonic, and correct regardless of how the
    # running count rises/falls (unlike running_count × total_elapsed). The head
    # bills the whole run; task instances only while RUNNING.
    now = time.time()
    dt_hr = (now - last_tick) / 3600.0
    last_tick = now
    cost_usd += (HEAD_RATE_USD_HR + running * TASK_RATE_USD_HR) * dt_hr

    # Data volumes from the trace rchar/wchar fields on the CALL_VARIANTS tasks.
    call_tasks = [t for t in tasks if is_call_task(t.get("name", ""))]
    bam_bytes = sum(parse_rchar(t.get("rchar", "0")) for t in call_tasks)
    vcf_bytes = sum(parse_rchar(t.get("wchar", "0")) for t in call_tasks)

    progress = {
        "status": "complete" if pipeline_done else "running",
        "started_at": started_at,
        "elapsed_seconds": now - started_at,
        "tasks_total": total,
        "tasks_running": running,
        "tasks_done": done,
        "tasks_failed": failed,
        "bam_bytes_read": bam_bytes,
        "vcf_bytes": vcf_bytes,
        "ec2_cost_usd": round(cost_usd, 6),
        "stats": read_stats_json() or {},
    }

    s3.put_object(Bucket=bucket, Key=prog_key, Body=json.dumps(progress))

    if pipeline_done:
        break
