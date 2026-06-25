"""Tests for the cost meter — pure logic, no AWS.

The cost meter is what backs the on-stage "$Y" claim, so its arithmetic is
covered directly: per-row cost, accumulation, monotonicity, fallback rates,
and the receipt shape.
"""

from omics_demo.cost import CostMeter, CostRow


def _meter():
    return CostMeter(
        rates={"c7g.2xlarge": 0.289, "c7g.large": 0.0723, "r7g.2xlarge": 0.4284},
        default_usd_per_hour=0.40,
    )


def test_instance_time_cost_arithmetic():
    m = _meter()
    # 30 CALL_VARIANTS tasks, 120s each → 3600 instance-seconds on c7g.2xlarge.
    usd = m.add_instance_time("CALL_VARIANTS ×30", "c7g.2xlarge", seconds=3600, instances=30)
    assert round(usd, 6) == round(3600 / 3600 * 0.289, 6) == 0.289


def test_total_accumulates_and_is_monotonic():
    m = _meter()
    assert m.total == 0.0
    m.add_instance_time("head", "c7g.large", seconds=600)  # 10 min head
    t1 = m.total
    m.add_instance_time("CALL_VARIANTS ×30", "c7g.2xlarge", seconds=3600, instances=30)
    t2 = m.total
    m.add_instance_time("MERGE_VCFS", "r7g.2xlarge", seconds=90)
    t3 = m.total
    assert t1 < t2 < t3  # adding rows only ever increases the total
    assert round(t3, 6) == round(600 / 3600 * 0.0723 + 0.289 + 90 / 3600 * 0.4284, 6)


def test_unknown_instance_uses_fallback_rate():
    m = _meter()
    usd = m.add_instance_time("weird", "x9z.42xlarge", seconds=3600)
    assert usd == 0.40  # default_usd_per_hour, not a silent $0


def test_rate_lookup():
    m = _meter()
    assert m.rate("c7g.2xlarge") == 0.289
    assert m.rate("unknown") == 0.40


def test_receipt_shape_and_rounding():
    m = _meter()
    m.add_instance_time("head (c7g.large)", "c7g.large", seconds=500, instances=1)
    r = m.receipt()
    assert set(r) == {"rows", "total"}
    assert len(r["rows"]) == 1
    row = r["rows"][0]
    assert row["step"] == "head (c7g.large)"
    assert row["instance_type"] == "c7g.large"
    assert row["instances"] == 1
    assert row["seconds"] == 500.0
    assert row["usd"] == round(500 / 3600 * 0.0723, 6)
    assert r["total"] == row["usd"]


def test_costrow_is_plain_dataclass():
    # Receipt rows must be JSON-friendly scalars (no lock leaking in).
    row = CostRow("s", "c7g.large", 1, 100.0, 0.0723, 0.002)
    assert isinstance(row.seconds, float)
