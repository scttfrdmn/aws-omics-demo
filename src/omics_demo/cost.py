"""
cost.py  --  the live cost meter for the variant-calling demo.

Pure Python: instance-seconds and per-hour rates in, dollars out. No AWS, no
network I/O — so the entire cost-accounting layer is trivially testable without
cloud credentials (see tests/test_cost.py). Mirrors the agentcore demo's CostMeter
pattern, but the priced unit here is EC2 INSTANCE-TIME (not LLM tokens).

What gets measured:

  Head node:   one long-running instance for the whole run. Cost = its billed
               wall-clock (launch→now) × its on-demand $/hr.
  Task fleet:  each pipeline task (CALL_VARIANTS / MERGE_VCFS / VCF_STATS) runs on
               its own ephemeral instance via nf-spawn. Cost = Σ over tasks of
               (that task's billed seconds × its instance $/hr).

Why an ACCUMULATING meter, not `running_tasks × elapsed × rate`:
  The naive instantaneous formula (current running-task count × total elapsed
  time) is wrong two ways — it assumes every currently-running task ran for the
  whole window, and it DROPS as tasks finish (running count falls). For a number
  you put on a slide ("$Y"), cost must be MONOTONIC and reflect actual billed
  instance-seconds. So we accumulate one row per task as it completes (with its
  real runtime), plus the head's live-elapsed cost. total() never decreases.

Per-second billing:
  EC2 Linux on-demand bills per second (60s minimum). We compute seconds × $/hr
  / 3600 directly — the dominant term at N=100 is the task fleet, and each task
  runs minutes, well past the 60s floor.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field


@dataclass
class CostRow:
    """One line on the receipt — a single instance's billed time.

    A task row is appended when its instance's runtime is known (task completed,
    or finalised at end-of-run). The head row is recomputed live from elapsed time.
    """

    step: str  # e.g. "CALL_VARIANTS ×30", "head (c7g.large)"
    instance_type: str  # e.g. "c7g.2xlarge"
    instances: int  # how many instances this row aggregates (1 for head)
    seconds: float  # total billed instance-seconds across those instances
    usd_per_hour: float  # on-demand rate for the instance type
    usd: float  # computed cost = seconds / 3600 × usd_per_hour × ... (see add_*)


@dataclass
class CostMeter:
    """Accumulates EC2 instance-time cost across one demo run.

    Constructed fresh per run. Rates come from pricing.instance_rates() (live
    truffle / AWS Price List, with config fallback). Thread-safe appends so the
    head poller and any concurrent updater never race the rows list.
    """

    # instance_type -> on-demand USD/hour
    rates: dict[str, float] = field(default_factory=dict)
    # Fallback rate if an instance type isn't in `rates` (keeps the meter honest
    # rather than silently zero — a visible over-estimate beats a hidden $0).
    default_usd_per_hour: float = 0.40

    rows: list[CostRow] = field(default_factory=list)
    _lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False, compare=False
    )

    def rate(self, instance_type: str) -> float:
        """On-demand USD/hour for an instance type (fallback if unknown)."""
        return self.rates.get(instance_type, self.default_usd_per_hour)

    def add_instance_time(
        self, step: str, instance_type: str, seconds: float, instances: int = 1
    ) -> float:
        """Record billed instance-time for one or more instances of a type.

        Cost = (seconds / 3600) × rate(instance_type). `seconds` is the TOTAL
        across `instances` (e.g. 30 tasks × 120s each → seconds=3600, instances=30).

        Returns the cost of this row in USD.
        """
        usd = (seconds / 3600.0) * self.rate(instance_type)
        with self._lock:
            self.rows.append(
                CostRow(step, instance_type, instances, seconds, self.rate(instance_type), usd)
            )
        return usd

    @property
    def total(self) -> float:
        """Running total of all recorded rows, in USD. Monotonic as rows are added."""
        return sum(r.usd for r in self.rows)

    def receipt(self) -> dict:
        """JSON-serialisable receipt for the UI + terminal runner.

        Rendered as the final itemised table and the headline "$Y" total.
        """
        return {
            "rows": [
                {
                    "step": r.step,
                    "instance_type": r.instance_type,
                    "instances": r.instances,
                    "seconds": round(r.seconds, 1),
                    "usd_per_hour": round(r.usd_per_hour, 4),
                    "usd": round(r.usd, 6),
                }
                for r in self.rows
            ],
            "total": round(self.total, 6),
        }
