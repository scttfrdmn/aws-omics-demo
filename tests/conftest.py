"""
conftest.py  --  shared fixtures for the 1000 Genomes variant-calling test suite.

No AWS calls are made in tests.  All AWS interactions are replaced with
fakes passed via dependency injection (same pattern as the PCSK9 demo).
"""

from __future__ import annotations

import pytest


@pytest.fixture
def sample_summary() -> dict:
    """A realistic pipeline summary dict for testing synthesis."""
    return {
        "total_samples": 30,
        "completed": 30,
        "elapsed_seconds": 480.0,
        "ec2_cost_usd": 0.02304,
        "super_populations": {"AFR": 10, "EUR": 10, "EAS": 10},
        "vcf_stats": {
            "total_records": 0,
            "snps": 0,
            "indels": 0,
            "ti_tv_ratio": 0.0,
        },
        "data_volumes": {
            "bam_bytes_read": 30_000_000_000,
            "vcf_bytes": 60_000_000,
        },
    }
