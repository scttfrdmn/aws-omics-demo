"""
test_worker_script.py  --  tests for head node script generation.

No AWS calls.  Verifies that render() substitutes config values correctly.
render() takes four args: cfg, nf_config_key, srr_list_key, main_nf_key.
"""

from __future__ import annotations

import types

import pytest

from omics_demo.worker_script import render

NF_CFG_KEY = "config/test-job/nextflow.config"
SRR_KEY = "slices/test-job/sample_list.json"
MAIN_NF_KEY = "pipeline/test-job/main.nf"


@pytest.fixture
def mock_cfg():
    return types.SimpleNamespace(
        BUCKET="test-bucket",
        REGION="us-east-1",
        JOB_NAME="test-job",
        INSTANCE_COUNT=4,
    )


def test_render_substitutes_bucket(mock_cfg):
    script = render(mock_cfg, NF_CFG_KEY, SRR_KEY, MAIN_NF_KEY)
    assert "test-bucket" in script


def test_render_substitutes_region(mock_cfg):
    script = render(mock_cfg, NF_CFG_KEY, SRR_KEY, MAIN_NF_KEY)
    assert "us-east-1" in script


def test_render_substitutes_job_name(mock_cfg):
    script = render(mock_cfg, NF_CFG_KEY, SRR_KEY, MAIN_NF_KEY)
    assert "test-job" in script


def test_render_substitutes_slice_key(mock_cfg):
    script = render(mock_cfg, NF_CFG_KEY, "slices/test-job/sample_list_002.json", MAIN_NF_KEY)
    assert "sample_list_002.json" in script


def test_render_reads_from_1000genomes(mock_cfg):
    """Head script must reference the 1000 Genomes bucket — CALL_VARIANTS pulls BAMs there."""
    script = render(mock_cfg, NF_CFG_KEY, SRR_KEY, MAIN_NF_KEY)
    assert "1000genomes" in script


def test_render_is_bash(mock_cfg):
    script = render(mock_cfg, NF_CFG_KEY, SRR_KEY, MAIN_NF_KEY)
    assert script.startswith("#!/bin/bash")


def test_render_has_completion_signal(mock_cfg):
    """Head must touch /tmp/SPAWN_COMPLETE so spawn knows it's done."""
    script = render(mock_cfg, NF_CFG_KEY, SRR_KEY, MAIN_NF_KEY)
    assert "SPAWN_COMPLETE" in script


def test_render_runs_nextflow(mock_cfg):
    """Head script must invoke nextflow run."""
    script = render(mock_cfg, NF_CFG_KEY, SRR_KEY, MAIN_NF_KEY)
    assert "nextflow run" in script


def test_render_uses_main_nf(mock_cfg):
    """Nextflow run command must reference the custom main.nf pipeline."""
    script = render(mock_cfg, NF_CFG_KEY, SRR_KEY, MAIN_NF_KEY)
    assert "main.nf" in script
    assert "nextflow run /tmp/nf-head/main.nf" in script


def test_main_nf_contains_variant_calling_stages():
    """The embedded main.nf must include the variant-calling DAG processes."""
    from omics_demo.worker_script import _MAIN_NF

    assert "CALL_VARIANTS" in _MAIN_NF
    assert "MERGE_VCFS" in _MAIN_NF
    assert "VCF_STATS" in _MAIN_NF
    # CALL_VARIANTS reads BAMs from the 1000 Genomes Open Data bucket and calls
    # variants with bcftools.
    assert "1000genomes" in _MAIN_NF
    assert "bcftools" in _MAIN_NF
