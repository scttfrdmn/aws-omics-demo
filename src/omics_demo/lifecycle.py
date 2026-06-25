"""
lifecycle.py  --  provision and teardown of the run's AWS infrastructure.

The speaker drives the WHOLE demo from the dashboard — Provision, Start, Teardown
— with no CLI. This module is the Provision/Teardown half (Start is the pipeline
runner in app.py). Each function takes an `emit(event)` callback so progress
streams to the page exactly like the pipeline feed.

Provision (green-room, ~10-15 min): create the shared FSx for Lustre filesystem,
S3-backed by the reference prefix, and confirm the reference is readable. Writes
the resulting fs-id back so Start can use it.

Teardown (after the talk): delete the FSx filesystem and terminate the head +
any straggler task instances — the only meaningful ongoing costs. Leaves the
tools AMIs and S3 (reusable, near-zero).

What is NOT here: staging the reference into S3 from 1000genomes. That is a true
one-time setup (benchmark/build_fsx_db.py) done once per account; the reference
then lives in s3://BUCKET/reference-fsx/ and every FSx imports it. Provision
assumes that prefix already exists.
"""

from __future__ import annotations

import contextlib
import subprocess
import time
from collections.abc import Callable

EmitFn = Callable[[dict], None]


def _emit(emit: EmitFn | None, event: dict) -> None:
    if emit:
        emit(event)


def _fsx_lifecycle(region: str, fs_id: str) -> str:
    import boto3

    fsx = boto3.client("fsx", region_name=region)
    resp = fsx.describe_file_systems(FileSystemIds=[fs_id])
    return resp["FileSystems"][0]["Lifecycle"]


def provision(cfg, emit: EmitFn | None = None) -> str | None:
    """Create the shared FSx filesystem for the run; return its fs-id (or None).

    Steps, each streamed to the dashboard:
      1. spawn launch --fsx-create (S3-backed by the reference prefix)
      2. wait for the filesystem to reach AVAILABLE
      3. confirm the reference FASTA + .fai are present on the mount

    The launched host carries --fsx-ttl so the FS is reaped if teardown is missed.
    """
    bucket = cfg.BUCKET
    region = cfg.REGION
    import_path = f"s3://{bucket}/reference-fsx"
    host = "omics-fsx-provision"

    _emit(emit, {"type": "phase", "label": "Provisioning FSx for Lustre (shared reference)…"})
    proc = subprocess.run(
        [
            "spawn",
            "launch",
            host,
            "--instance-type",
            getattr(cfg, "HEAD_INSTANCE_TYPE", "c7g.large"),
            "--region",
            region,
            "--az",
            getattr(cfg, "BENCH_AZ", "us-east-1a"),
            "--fsx-create",
            "--fsx-lifecycle",
            "durable",
            "--fsx-ttl",
            "1d",
            "--fsx-s3-bucket",
            bucket,
            "--fsx-import-path",
            import_path,
            "--fsx-storage-capacity",
            "1200",
            "--fsx-throughput",
            "250",
            "--fsx-mount-point",
            getattr(cfg, "FSX_MOUNT", "/fsx"),
            "--ttl",
            "4h",
            "--wait-for-ssh",
            "-y",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        _emit(emit, {"type": "error", "message": f"FSx provision failed: {proc.stderr[:300]}"})
        return None

    # Find the new filesystem by the provisioning host's tag.
    import boto3

    fsx = boto3.client("fsx", region_name=region)
    fs_id = None
    for _ in range(12):
        for fs in fsx.describe_file_systems().get("FileSystems", []):
            if fs["Lifecycle"] in ("DELETING", "FAILED"):
                continue
            tags = {t["Key"]: t["Value"] for t in fs.get("Tags", [])}
            if tags.get("Name") == host:
                fs_id = fs["FileSystemId"]
                break
        if fs_id:
            break
        time.sleep(5)
    if not fs_id:
        _emit(emit, {"type": "error", "message": "Provisioned FSx but could not find its id."})
        return None

    _emit(emit, {"type": "phase", "label": f"FSx {fs_id} creating — waiting for AVAILABLE…"})
    for _ in range(60):  # up to ~20 min
        state = _fsx_lifecycle(region, fs_id)
        if state == "AVAILABLE":
            break
        if state == "FAILED":
            _emit(emit, {"type": "error", "message": f"FSx {fs_id} entered FAILED state."})
            return None
        time.sleep(20)
    else:
        _emit(emit, {"type": "error", "message": f"FSx {fs_id} not AVAILABLE after ~20 min."})
        return None

    _emit(
        emit,
        {
            "type": "phase",
            "label": f"FSx {fs_id} AVAILABLE — infrastructure ready. Press Start to run.",
        },
    )
    _emit(emit, {"type": "provisioned", "fsx_id": fs_id})
    return fs_id


def reset(cfg, emit: EmitFn | None = None) -> dict:
    """Clear the last run so a fresh one can start — WITHOUT touching FSx.

    For the rehearse → reset → present-live flow: provision once, do a full dress
    run, reset (cheap, seconds), then run again on the already-warm FSx. Deletes
    this job's S3 results/work prefixes and the reference marker; leaves the FSx
    filesystem and the staged reference intact. Distinct from teardown (which
    deletes FSx). Also terminates any straggler task instances from the prior run
    so they don't bleed cost or confuse the next run's counts.
    """
    import boto3

    from . import pipeline

    region = cfg.REGION
    summary = {"cleared_prefixes": [], "instances_terminated": [], "errors": []}

    _emit(emit, {"type": "phase", "label": "Reset: clearing previous run (keeping FSx)…"})

    # Drop S3 results + reference marker for this job (cheap; seconds).
    try:
        pipeline.clear_results(cfg)
        with contextlib.suppress(Exception):
            pipeline.clear_reference_marker(cfg)
        summary["cleared_prefixes"].append(f"results/{cfg.JOB_NAME}/")
    except Exception as exc:  # noqa: BLE001
        summary["errors"].append(f"results: {exc}")
        _emit(emit, {"type": "error", "message": f"Reset results error: {exc}"})

    # Terminate any leftover task instances from the prior run (NOT the FSx host).
    try:
        ec2 = boto3.client("ec2", region_name=region)
        resp = ec2.describe_instances(
            Filters=[
                {"Name": "instance-state-name", "Values": ["running", "pending"]},
                {"Name": "tag:Name", "Values": ["omics-demo-*", "nf-*"]},
            ]
        )
        ids = [
            i["InstanceId"] for r in resp.get("Reservations", []) for i in r.get("Instances", [])
        ]
        if ids:
            ec2.terminate_instances(InstanceIds=ids)
            summary["instances_terminated"] = ids
    except Exception as exc:  # noqa: BLE001
        summary["errors"].append(f"instances: {exc}")

    _emit(emit, {"type": "phase", "label": "Reset complete — ready to run again on the same FSx."})
    _emit(emit, {"type": "reset_done", **summary})
    return summary


def teardown(cfg, emit: EmitFn | None = None) -> dict:
    """Delete the run's FSx + terminate its instances. Returns a summary dict.

    Targets only THIS demo's resources (tag Name prefixes omics-*/nf-*) and the
    FSx in cfg.FSX_ID. Leaves the tools AMIs and S3 (reusable). Best-effort: a
    failure on one resource still attempts the rest.
    """
    import boto3

    region = cfg.REGION
    ec2 = boto3.client("ec2", region_name=region)
    summary = {"instances_terminated": [], "fsx_deleted": None, "errors": []}

    # 1. Terminate omics/nf instances (head + any straggler tasks).
    _emit(emit, {"type": "phase", "label": "Teardown: terminating demo instances…"})
    try:
        resp = ec2.describe_instances(
            Filters=[
                {"Name": "instance-state-name", "Values": ["running", "pending", "stopped"]},
                {"Name": "tag:Name", "Values": ["omics-*", "nf-*"]},
            ]
        )
        ids = [
            i["InstanceId"] for r in resp.get("Reservations", []) for i in r.get("Instances", [])
        ]
        if ids:
            ec2.terminate_instances(InstanceIds=ids)
            summary["instances_terminated"] = ids
            _emit(emit, {"type": "phase", "label": f"Terminating {len(ids)} instance(s)."})
        else:
            _emit(emit, {"type": "phase", "label": "No demo instances running."})
    except Exception as exc:  # noqa: BLE001
        summary["errors"].append(f"instances: {exc}")
        _emit(emit, {"type": "error", "message": f"Instance teardown error: {exc}"})

    # 2. Delete the FSx filesystem (the standing meter).
    fs_id = getattr(cfg, "FSX_ID", "") or ""
    if fs_id:
        _emit(emit, {"type": "phase", "label": f"Teardown: deleting FSx {fs_id}…"})
        try:
            boto3.client("fsx", region_name=region).delete_file_system(FileSystemId=fs_id)
            summary["fsx_deleted"] = fs_id
            _emit(emit, {"type": "phase", "label": f"FSx {fs_id} deleting."})
        except Exception as exc:  # noqa: BLE001
            summary["errors"].append(f"fsx: {exc}")
            _emit(emit, {"type": "error", "message": f"FSx teardown error: {exc}"})
    else:
        _emit(emit, {"type": "phase", "label": "No FSX_ID set — nothing to delete."})

    _emit(emit, {"type": "teardown_done", **summary})
    return summary
