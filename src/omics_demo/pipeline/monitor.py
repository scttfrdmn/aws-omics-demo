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
import re
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
# FSX_RATE = the shared FSx for Lustre filesystem's hourly storage charge, which
# bills the whole run window regardless of task count (a flat term in the integral)
# so the headline "$Y" is true all-in spend (EC2 + FSx), not compute-only.
HEAD_RATE_USD_HR = float("@@HEAD_RATE@@")
TASK_RATE_USD_HR = float("@@TASK_RATE@@")
FSX_RATE_USD_HR = float("@@FSX_RATE@@")

# Load the population map from the sample list so we can annotate stats later.
with open("/tmp/nf-head/sample_list.json") as f:
    sample_list = json.load(f)
pop_map = {item["sample_id"]: item.get("super_population", "unknown") for item in sample_list}


# The head's local .nextflow.log carries LIVE per-task lifecycle lines as they
# happen — unlike the S3 trace.tsv, which only finalises near the end on the spawn
# executor (so a trace-based monitor sits at 0/0 the whole run, then jumps to done).
# We parse the log for real-time counts so the dashboard actually animates.
_NF_LOG = "/tmp/nf-head/.nextflow.log"
_RE_SUBMIT = re.compile(r"Submitting task '([^']+)' to spawn instance '([^']+)'")
_RE_DONE = re.compile(r"Task '([^']+)' completed \(exit (\d+)\) on instance '([^']+)'")


def read_log_tasks():
    """Return (submitted, completed, failed) name-sets parsed from .nextflow.log.

    submitted/completed are sets of task display names (e.g. "CALL_VARIANTS (HG01879)")
    so running = submitted − completed and counts can't double-count on log replay.
    failed = completed with a non-zero exit code.
    """
    submitted, completed, failed = set(), set(), set()
    try:
        with open(_NF_LOG, errors="ignore") as f:
            for line in f:
                m = _RE_SUBMIT.search(line)
                if m:
                    submitted.add(m.group(1))
                    continue
                m = _RE_DONE.search(line)
                if m:
                    completed.add(m.group(1))
                    if m.group(2) != "0":
                        failed.add(m.group(1))
    except OSError:
        pass
    return submitted, completed, failed


def read_trace():
    # Parse the Nextflow trace TSV (used for byte counts; finalises late on spawn).
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

    # LIVE counts from .nextflow.log (real-time), restricted to the per-sample
    # CALL_VARIANTS fan-out — that's the "N genomes" the audience watches. The
    # downstream MERGE_VCFS/VCF_STATS are single tasks shown as separate phases.
    submitted, completed, failed_set = read_log_tasks()
    call_sub = {t for t in submitted if is_call_task(t)}
    call_done = {t for t in completed if is_call_task(t)}
    done = len(call_done)
    running = len(call_sub - call_done)
    failed = len({t for t in failed_set if is_call_task(t)})
    total = len(sample_list)  # known sample count — the denominator stays fixed

    # Per-super-population genomes-done, so the feed can say which populations are
    # landing (e.g. "AFR 8/10"). Sample name is "CALL_VARIANTS (HG01879)".
    def _sample_of(name):
        m = re.search(r"\(([^)]+)\)", name)
        return m.group(1) if m else ""

    pop_done: dict = {}
    for t in call_done:
        sp = pop_map.get(_sample_of(t), "unknown")
        pop_done[sp] = pop_done.get(sp, 0) + 1

    tasks = read_trace()  # still used for byte volumes (below)

    # ── Cost: integrate the burn rate over this poll interval ────────────────
    # At each tick, the instantaneous burn is the head + every currently-RUNNING
    # task instance. Accumulating (rate × dt) over the actual intervals gives the
    # real billed instance-seconds — monotonic, and correct regardless of how the
    # running count rises/falls (unlike running_count × total_elapsed). The head
    # bills the whole run; task instances only while RUNNING.
    now = time.time()
    dt_hr = (now - last_tick) / 3600.0
    last_tick = now
    cost_usd += (HEAD_RATE_USD_HR + running * TASK_RATE_USD_HR + FSX_RATE_USD_HR) * dt_hr

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
        "pop_done": pop_done,  # {super_pop: genomes_called} for live feed context
        "bam_bytes_read": bam_bytes,
        "vcf_bytes": vcf_bytes,
        "ec2_cost_usd": round(cost_usd, 6),
        "stats": read_stats_json() or {},
    }

    s3.put_object(Bucket=bucket, Key=prog_key, Body=json.dumps(progress))

    if pipeline_done:
        break
