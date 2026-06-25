"""
worker_script.py  --  ship the Nextflow head-node artifacts and render its bootstrap.

The pipeline itself lives in real, readable files under `pipeline/` — NOT as
strings in this module:
    pipeline/main.nf                 the Nextflow DAG (CALL_VARIANTS → MERGE_VCFS → VCF_STATS)
    pipeline/monitor.py              head-side progress reporter
    pipeline/head_bootstrap.sh.tmpl  cloud-init for the head instance
    pipeline/README.md               a researcher's guide to all of the above

This module just (a) renders the head bootstrap by substituting @@TOKEN@@ values
and (b) uploads each artifact to S3 so the head node can fetch them. See
pipeline/README.md for the architecture.

Architecture:
  app.py / run_headless.py launch ONE small head instance via spawn.
  The head runs Nextflow with the nf-spawn executor; nf-spawn dispatches each
  pipeline task to its own ephemeral EC2 instance (sized per process label),
  which self-terminates when the task completes. The head only orchestrates.
"""

from __future__ import annotations

import os
import tempfile

import boto3

# Real pipeline artifacts live alongside this module under pipeline/.
_PIPELINE_DIR = os.path.join(os.path.dirname(__file__), "pipeline")


def _read(name: str) -> str:
    with open(os.path.join(_PIPELINE_DIR, name)) as f:
        return f.read()


# @@TOKEN@@ placeholders substituted into the head bootstrap template.
_TOK_BUCKET = "@@BUCKET@@"
_TOK_REGION = "@@REGION@@"
_TOK_JOB_NAME = "@@JOB_NAME@@"
_TOK_NF_CFG = "@@NF_CONFIG_KEY@@"  # S3 key for the rendered nextflow.config
_TOK_SRR_KEY = "@@SRR_LIST_KEY@@"  # S3 key for the sample list JSON
_TOK_MAIN_NF = "@@MAIN_NF_KEY@@"  # S3 key for pipeline/main.nf
_TOK_MONITOR = "@@MONITOR_KEY@@"  # S3 key for pipeline/monitor.py


def render(cfg, nf_config_key: str, srr_list_key: str, main_nf_key: str = "") -> str:
    """Return the head-node bootstrap script with config values substituted.

    The pipeline (main.nf) and monitor (monitor.py) are fetched by the head from
    S3 at the keys returned by upload_main_nf()/upload_monitor(); their default
    keys mirror those functions so a bare render() still points at the right place.
    """
    return (
        _read("head_bootstrap.sh.tmpl")
        .replace(_TOK_BUCKET, cfg.BUCKET)
        .replace(_TOK_REGION, cfg.REGION)
        .replace(_TOK_JOB_NAME, cfg.JOB_NAME)
        .replace(_TOK_NF_CFG, nf_config_key)
        .replace(_TOK_SRR_KEY, srr_list_key)
        .replace(_TOK_MAIN_NF, main_nf_key or f"pipeline/{cfg.JOB_NAME}/main.nf")
        .replace(_TOK_MONITOR, f"pipeline/{cfg.JOB_NAME}/monitor.py")
    )


def upload_main_nf(cfg) -> str:
    """Upload pipeline/main.nf to S3 and return its key.

    The head downloads it and runs `nextflow run main.nf`. main.nf is a normal,
    standalone-runnable Nextflow file — see pipeline/README.md.
    """
    s3 = boto3.client("s3", region_name=cfg.REGION)
    key = f"pipeline/{cfg.JOB_NAME}/main.nf"
    s3.put_object(Bucket=cfg.BUCKET, Key=key, Body=_read("main.nf").encode())
    return key


def _cost_rates(cfg) -> tuple[float, float]:
    """(head $/hr, task $/hr) for the monitor's live cost integral.

    Real on-demand rates via pricing.instance_rates (live truffle + fallback).
    head = HEAD_INSTANCE_TYPE; task = the CALL_VARIANTS (process_medium) instance,
    the fan-out term that dominates cost at N=100.
    """
    from . import nextflow_config, pricing

    head_type = getattr(cfg, "HEAD_INSTANCE_TYPE", "c7g.large")
    labels = nextflow_config._LABEL_INSTANCE_TYPES_BY_ARCH[nextflow_config._arch(cfg)]
    task_type = labels["process_medium"]
    rates = pricing.instance_rates([head_type, task_type], cfg.REGION)
    return rates.get(head_type, 0.0723), rates.get(task_type, 0.289)


def upload_monitor(cfg) -> str:
    """Upload pipeline/monitor.py (with @@TOKEN@@ substituted) to S3, return key."""
    head_rate, task_rate = _cost_rates(cfg)
    body = (
        _read("monitor.py")
        .replace(_TOK_REGION, cfg.REGION)
        .replace(_TOK_BUCKET, cfg.BUCKET)
        .replace(_TOK_JOB_NAME, cfg.JOB_NAME)
        .replace("@@HEAD_RATE@@", f"{head_rate:.6f}")
        .replace("@@TASK_RATE@@", f"{task_rate:.6f}")
    )
    s3 = boto3.client("s3", region_name=cfg.REGION)
    key = f"pipeline/{cfg.JOB_NAME}/monitor.py"
    s3.put_object(Bucket=cfg.BUCKET, Key=key, Body=body.encode())
    return key


def upload_nextflow_config(cfg, nf_config_str: str) -> str:
    """Upload the rendered nextflow.config (from nextflow_config.render) to S3."""
    s3 = boto3.client("s3", region_name=cfg.REGION)
    key = f"config/{cfg.JOB_NAME}/nextflow.config"
    s3.put_object(Bucket=cfg.BUCKET, Key=key, Body=nf_config_str.encode())
    return key


def write_srr_slice(cfg, accessions: list[tuple[str, str, str, str]]) -> str:
    """Upload the sample list as a single JSON to S3; return its key.

    With nf-spawn, Nextflow manages parallelism — the head gets all samples and
    queueSize controls concurrency. Each entry is a 4-tuple
    (sample_id, population, super_population, bam_path) for one 1000 Genomes BAM.
    """
    import json

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
    s3.put_object(Bucket=cfg.BUCKET, Key=key, Body=json.dumps(entries, indent=2).encode())
    return key


def write_temp(script: str) -> str:
    """Write a rendered script to a NamedTemporaryFile and return its path."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=False) as f:
        f.write(script)
        return f.name
