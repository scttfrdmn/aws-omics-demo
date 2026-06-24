"""
test_truffle.py  --  tests for quota-based queueSize derivation.

No AWS calls, no truffle CLI invocations.
"""

from __future__ import annotations

from unittest.mock import patch

from omics_demo.truffle import (
    InstanceSpec,
    QuotaInfo,
    derive_queue_size,
    quota_summary,
)


def _quota(family: str, available: int) -> QuotaInfo:
    return QuotaInfo(
        family=family,
        region="us-east-1",
        limit=available,
        used=0,
        available=available,
    )


def _fake_specs(instance_types, region):
    # The variant-calling instance mix (arm64): VCF_STATS c7g.large,
    # CALL_VARIANTS c7g.2xlarge, MERGE_VCFS r7g.2xlarge.
    vcpus = {"c7g.large": 2, "c7g.2xlarge": 8, "r7g.2xlarge": 8}
    return {
        itype: InstanceSpec(itype, vcpus.get(itype, 2), 0.10)
        for itype in instance_types
        if itype in vcpus
    }


def test_derive_queue_size_basic():
    quotas = {"c7g": _quota("c7g", 192)}
    with patch("omics_demo.truffle.get_instance_specs", side_effect=_fake_specs):
        # 192 × 0.8 = 153 usable; largest = 8 vCPU → 19
        qs = derive_queue_size(quotas, ["c7g.2xlarge", "c7g.large"], "us-east-1")
    assert qs >= 4
    assert qs <= 30


def test_derive_queue_size_constrained():
    quotas = {"c7g": _quota("c7g", 8)}
    with patch("omics_demo.truffle.get_instance_specs", side_effect=_fake_specs):
        qs = derive_queue_size(quotas, ["c7g.2xlarge"], "us-east-1")
    assert qs == 4  # 8 × 0.8 = 6 / 8 = 0 → floor


def test_derive_queue_size_no_quotas():
    with patch("omics_demo.truffle.get_instance_specs", side_effect=_fake_specs):
        qs = derive_queue_size({}, ["c7g.2xlarge"], "us-east-1")
    assert qs == 4


def test_derive_queue_size_no_quotas_empty():
    # No quotas at all → floor
    with patch("omics_demo.truffle.get_instance_specs", return_value={}):
        qs = derive_queue_size({}, ["c7g.2xlarge"], "us-east-1")
    assert qs == 4


def test_quota_summary_with_data():
    quotas = {"c7g": _quota("c7g", 192)}
    summary = quota_summary(quotas, 12)
    assert "12" in summary
    assert "192" in summary


def test_quota_summary_empty():
    summary = quota_summary({}, 4)
    assert "4" in summary
    assert "unavailable" in summary
