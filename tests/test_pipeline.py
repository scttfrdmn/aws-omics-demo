"""
test_pipeline.py  --  tests for pipeline monitoring helpers.

No AWS calls.  Tests focus on pure logic (cost calculation, elapsed formatting).
"""

from __future__ import annotations

from omics_demo.pipeline import DataVolume, _fmt_elapsed


def test_fmt_elapsed_seconds():
    assert _fmt_elapsed(45) == "45s"


def test_fmt_elapsed_minutes():
    assert _fmt_elapsed(125) == "2m 5s"


def test_fmt_elapsed_zero():
    assert _fmt_elapsed(0) == "0s"


def test_data_volume_compression_ratio():
    # A large BAM streams through into a tiny VCF.
    dv = DataVolume(bam_bytes_read=30_000_000_000, vcf_bytes=60_000_000)
    assert abs(dv.compression_ratio - 500.0) < 0.001


def test_data_volume_compression_ratio_zero():
    dv = DataVolume(bam_bytes_read=0, vcf_bytes=0)
    assert dv.compression_ratio == 0.0


def test_data_volume_gb():
    dv = DataVolume(bam_bytes_read=2_000_000_000, vcf_bytes=7_000_000)
    assert abs(dv.bam_gb - 2.0) < 0.01
    assert abs(dv.vcf_gb - 0.007) < 0.001
