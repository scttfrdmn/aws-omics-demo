"""
pipeline.py  --  poll S3 for Nextflow pipeline progress.

The head node writes progress.json every 15 seconds.  This module reads
it and returns a structured snapshot the dashboard can render.

Data volume tracking:
  - bam_bytes_read:    bytes pulled from s3://1000genomes/ (low-coverage BAMs)
  - vcf_bytes:         bytes written as per-sample VCFs by CALL_VARIANTS
  - reference_bytes_read: bytes read from the shared reference genome (FSx/EBS)

  vcf_bytes / bam_bytes_read  ≈  the variant-call compression ratio — a 30 GB
  BAM yields a few-MB chr20 VCF.  reference_bytes_read shows how much of the
  shared reference each CALL_VARIANTS task touched (read in place off FSx/EBS).
"""

from __future__ import annotations

import csv
import io
import json
import time
from dataclasses import dataclass, field
from typing import Any

import boto3

from . import truffle as _truffle


@dataclass
class DataVolume:
    """Bytes moved at each stage of the pipeline."""

    bam_bytes_read: int = 0  # pulled from s3://1000genomes (BAM format)
    vcf_bytes: int = 0  # per-sample VCFs emitted by CALL_VARIANTS
    reference_bytes_read: int = 0  # read from the shared reference (FSx/EBS)

    @property
    def compression_ratio(self) -> float:
        """BAM → VCF size ratio (a 30 GB BAM → few-MB chr20 VCF)."""
        if self.vcf_bytes == 0:
            return 0.0
        return self.bam_bytes_read / self.vcf_bytes

    @property
    def bam_gb(self) -> float:
        return self.bam_bytes_read / 1e9

    @property
    def vcf_gb(self) -> float:
        return self.vcf_bytes / 1e9


@dataclass
class PipelineProgress:
    """Live snapshot of pipeline execution."""

    status: str = "idle"  # idle | running | complete | error
    elapsed_seconds: float = 0.0
    tasks_total: int = 0
    tasks_running: int = 0
    tasks_done: int = 0
    tasks_failed: int = 0
    queue_size: int = 0  # Nextflow queueSize (from truffle quota)
    ec2_cost_usd: float = 0.0
    data: DataVolume = field(default_factory=DataVolume)
    variant_counts: dict[str, list[str]] = field(default_factory=dict)

    @property
    def concurrency_pct(self) -> float:
        """tasks_running / queue_size as a 0-1 fraction."""
        if self.queue_size == 0:
            return 0.0
        return min(1.0, self.tasks_running / self.queue_size)

    @property
    def completion_pct(self) -> float:
        if self.tasks_total == 0:
            return 0.0
        return self.tasks_done / self.tasks_total


def poll_progress(cfg, start_time: float, queue_size: int) -> PipelineProgress:
    """Read progress.json from S3 and return a PipelineProgress snapshot.

    Args:
        cfg:        config module (REGION, BUCKET, JOB_NAME).
        start_time: time.time() when the head instance was launched.
        queue_size: queueSize derived from truffle quota query.
    """
    s3 = boto3.client("s3", region_name=cfg.REGION)
    raw = _safe_get(s3, cfg.BUCKET, f"results/{cfg.JOB_NAME}/progress.json")

    elapsed = time.time() - start_time

    head_type = getattr(cfg, "HEAD_INSTANCE_TYPE", "c7g.large")
    head_spec = _truffle.get_instance_spec(head_type, cfg.REGION)
    head_price = head_spec.on_demand_price_usd if head_spec else 0.0363

    p = PipelineProgress(
        queue_size=queue_size,
        elapsed_seconds=elapsed,
    )

    if raw is None:
        p.ec2_cost_usd = (elapsed / 3600) * head_price
        return p

    p.status = raw.get("status", "running")
    p.tasks_total = raw.get("tasks_total", 0)
    p.tasks_running = raw.get("tasks_running", 0)
    p.tasks_done = raw.get("tasks_done", 0)
    p.tasks_failed = raw.get("tasks_failed", 0)

    # Cost: head node + actually-running task instances.
    # Use blended average of task instance prices from truffle.
    from . import nextflow_config as _nfc

    task_specs = _truffle.get_instance_specs(_nfc.all_instance_types(cfg), cfg.REGION)
    avg_task_price = (
        sum(s.on_demand_price_usd for s in task_specs.values()) / len(task_specs)
        if task_specs
        else 0.0
    )
    p.ec2_cost_usd = (elapsed / 3600) * (head_price + p.tasks_running * avg_task_price)

    # data_volumes key used in summary.json; flat in progress.json
    dv = raw.get("data_volumes") or raw
    p.data = DataVolume(
        bam_bytes_read=dv.get("bam_bytes_read", 0),
        vcf_bytes=dv.get("vcf_bytes", 0),
        reference_bytes_read=dv.get("reference_bytes_read", 0),
    )

    p.variant_counts = _sample_variants(s3, cfg.BUCKET, cfg.JOB_NAME)

    return p


def read_summary(cfg) -> dict[str, Any] | None:
    """Return summary.json if it exists, else None."""
    s3 = boto3.client("s3", region_name=cfg.REGION)
    return _safe_get(s3, cfg.BUCKET, f"results/{cfg.JOB_NAME}/summary.json")


def is_pipeline_complete(cfg) -> bool:
    """Return True if the head node has written a genuine complete status to S3.

    Requires status=="complete" AND tasks_done > 0 to avoid false positives
    from stale progress.json files left by previous failed runs.
    """
    s3 = boto3.client("s3", region_name=cfg.REGION)
    prog = _safe_get(s3, cfg.BUCKET, f"results/{cfg.JOB_NAME}/progress.json")
    if prog is None:
        return False
    return prog.get("status") == "complete" and prog.get("tasks_done", 0) > 0


def clear_results(cfg) -> None:
    """Delete the results prefix for this job so stale data doesn't mislead polling."""
    s3 = boto3.client("s3", region_name=cfg.REGION)
    prefix = f"results/{cfg.JOB_NAME}/"
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=cfg.BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            s3.delete_object(Bucket=cfg.BUCKET, Key=obj["Key"])


# ── sample CSV (samples.csv consumed by the custom main.nf) ──────────────────


def samples_prefix(cfg) -> str:
    """S3 key prefix holding this job's input samplesheet (no trailing slash)."""
    return f"input/{cfg.JOB_NAME}"


def samples_s3_uri(cfg) -> str:
    """The s3:// URI of the samples CSV that main.nf reads as params.samples."""
    return f"s3://{cfg.BUCKET}/{samples_prefix(cfg)}/samples.csv"


def write_samples_csv(cfg, samples: list[tuple[str, str, str, str]]) -> str:
    """Write the samples CSV main.nf consumes and return its s3:// URI.

    Columns: sample_id,population,super_population,bam_path — one row per
    1000 Genomes sample. CALL_VARIANTS keys on sample_id and reads bam_path
    directly from s3://1000genomes. The population columns ride through so
    VCF_STATS / analyze_study.py can group allele frequencies by super_population.
    """
    s3 = boto3.client("s3", region_name=cfg.REGION)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["sample_id", "population", "super_population", "bam_path"])
    for sample_id, population, super_population, bam_path in samples:
        writer.writerow([sample_id, population, super_population, bam_path])
    s3.put_object(
        Bucket=cfg.BUCKET,
        Key=f"{samples_prefix(cfg)}/samples.csv",
        Body=buf.getvalue().encode(),
    )
    return samples_s3_uri(cfg)


# ── reference-genome delivery marker (s3:// marker for FSx-delivered ref) ────


def reference_marker_prefix(cfg) -> str:
    """S3 key prefix holding the reference db_path marker for this job."""
    return f"reference/{cfg.JOB_NAME}"


def reference_marker_s3_uri(cfg, ref_name: str = "reference") -> str:
    """The s3:// URI main.nf points params.reference at for the shared genome.

    The basename (e.g. 'reference') MUST equal the ext.fsx/ext.volumes mount
    basename so nf-spawn (#55) symlinks the staged input to the mounted FSx/EBS
    reference on the task instead of downloading — zero copy. The object at this
    URI is a tiny MARKER, never the genome: it exists only so the head-side
    exists-check passes against the s3:// workDir filesystem (so Nextflow does
    NOT foreign-copy a head-local path up to S3 and deadlock). The real ~3 GB
    reference + .fai are read in place off the volume via the symlink.
    """
    return f"s3://{cfg.BUCKET}/{reference_marker_prefix(cfg)}/{ref_name}"


def write_reference_marker(cfg, ref_name: str = "reference") -> None:
    """Create the marker object backing the reference db_path s3:// URI.

    main.nf types params.reference as a path that must exist, so the URI must
    resolve to something. We write a zero-byte marker AT the reference key
    itself; the task-side symlink replaces it with the real mounted FSx/EBS
    reference, so the marker content is never read.
    """
    s3 = boto3.client("s3", region_name=cfg.REGION)
    s3.put_object(
        Bucket=cfg.BUCKET,
        Key=f"{reference_marker_prefix(cfg)}/{ref_name}",
        Body=b"nf-spawn ext.fsx/ext.volumes marker - real reference is on the mounted volume\n",
    )


def clear_reference_marker(cfg) -> None:
    """Delete this job's reference db_path marker object (cleanup after a run)."""
    s3 = boto3.client("s3", region_name=cfg.REGION)
    prefix = f"{reference_marker_prefix(cfg)}/"
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=cfg.BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            s3.delete_object(Bucket=cfg.BUCKET, Key=obj["Key"])


def _safe_get(s3, bucket: str, key: str) -> dict | None:
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def _sample_variants(s3, bucket: str, job_name: str) -> dict[str, list[str]]:
    """Return up to 3 headline variant facts per super-population.

    Reads the per-sample variant JSONs CALL_VARIANTS lands under
    results/<job>/variants/. Keyed by super_population so the dashboard can show
    that samples cluster by group (the population-differentiation QC signal).
    """
    facts: dict[str, list[str]] = {}
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=bucket,
            Prefix=f"results/{job_name}/variants/",
            PaginationConfig={"MaxItems": 20},
        ):
            for obj in page.get("Contents", []):
                data = _safe_get(s3, bucket, obj["Key"])
                if not data:
                    continue
                group = data.get("super_population", "unknown")
                top = data.get("top_variants", [])[:3]
                if top and group not in facts:
                    facts[group] = top
    except Exception:  # noqa: BLE001
        pass
    return facts


def _fmt_elapsed(seconds: float) -> str:
    """Format elapsed seconds as 'Xm Ys' or 'Xs'."""
    s = int(seconds)
    m, sec = divmod(s, 60)
    if m:
        return f"{m}m {sec}s"
    return f"{sec}s"
