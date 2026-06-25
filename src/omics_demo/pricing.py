"""
pricing.py  --  live EC2 on-demand rates for the cost meter.

Fetches current per-hour on-demand prices for the instance types the pipeline
uses, so the cost meter shows REAL prices without a config edit. Called once at
run start; the result seeds cost.CostMeter(rates=...).

Mirrors the agentcore demo's pricing.py: a live fetch with a documented fallback
chain, isolated from the web layer and the fake path.

Source: omics_demo.truffle (the spore.host `truffle spot`/pricing CLI), which is
already the demo's instance-pricing oracle. We keep the same fallback discipline:

  1. Live rate from truffle for each instance type.
  2. config.INSTANCE_RATES_USD_HR[type] if truffle is unavailable for that type.
  3. cost.CostMeter.default_usd_per_hour as the last-resort floor.

The fake path (DEMO_FAKE / fakes.py) never calls this — it uses static rates so
rehearsal needs no AWS.
"""

from __future__ import annotations

# Hard fallback rates (us-east-1 on-demand, approximate) for the matched-pair
# instance families the pipeline uses. Only consulted if truffle can't answer.
# Verified ~2026-06; refresh if AWS repricing drifts these materially.
_FALLBACK_USD_HR: dict[str, float] = {
    # arm64 (Graviton)
    "c7g.large": 0.0723,
    "c7g.2xlarge": 0.2890,
    "r7g.2xlarge": 0.4284,
    # x86 (matched pairs)
    "c7i.large": 0.0893,
    "c7i.2xlarge": 0.3570,
    "r7i.2xlarge": 0.5292,
    # small head fallback
    "t4g.small": 0.0168,
}


def instance_rates(instance_types: list[str], region: str) -> dict[str, float]:
    """Return {instance_type: on_demand_usd_per_hour} for the given types.

    Live truffle lookup per type, falling back to _FALLBACK_USD_HR (then the
    CostMeter default for anything still missing). Never raises — a degraded
    rate table is better than a crashed demo at launch time.
    """
    from . import truffle

    rates: dict[str, float] = {}
    specs: dict = {}
    try:
        specs = truffle.get_instance_specs(instance_types, region)
    except Exception:
        specs = {}
    for itype in instance_types:
        spec = specs.get(itype)
        if spec is not None and getattr(spec, "on_demand_price_usd", 0) > 0:
            rates[itype] = float(spec.on_demand_price_usd)
        elif itype in _FALLBACK_USD_HR:
            rates[itype] = _FALLBACK_USD_HR[itype]
    return rates
