"""
agent.py  --  Bedrock Sonnet synthesis of variant-calling results.

After the pipeline finishes, this module calls Bedrock to generate
plain-language population-genetics insights from the cohort VCF stats.

The synthesis follows a single pattern: feed the structured results
(per-super-population sample counts + cohort variant QC: Ti/Tv ratio,
SNP/indel counts) to Claude Sonnet and ask for three key insights a
researcher would care about.

Design notes:
  - Takes a backend by dependency injection (like the PCSK9 demo) so
    tests can pass a fake without hitting AWS.
  - Emits events compatible with app.py's WebSocket protocol.
  - Cost is tracked and emitted exactly like the PCSK9 demo.
"""

from __future__ import annotations

from collections.abc import Callable

import boto3

# Pricing: Claude Sonnet 4.6 on Bedrock (us-east-1, as of 2026-05)
# Source: Bedrock console pricing page
_SONNET_INPUT_USD_PER_1K = 0.003  # $3.00 / 1M input tokens
_SONNET_OUTPUT_USD_PER_1K = 0.015  # $15.00 / 1M output tokens

_SYNTHESIS_SYSTEM = """\
You are a population geneticist summarizing results from a bcftools germline
variant-calling study of 1000 Genomes Project low-coverage samples (chromosome
20), drawn balanced across three super-populations (AFR / EUR / EAS).

Write three numbered insights for a research-computing conference audience:
  1. The most striking variant pattern (counts, SNP/indel split) across the cohort.
  2. A variant-quality finding — comment on the Ti/Tv (transition/transversion)
     ratio, which is ~2.0-2.1 genome-wide for human WGS, and what it implies.
  3. A methodological observation about the pipeline performance or data quality.

Each insight should be one or two sentences.  Be specific: cite variant counts,
Ti/Tv values, or sample counts where relevant.  Do not speculate beyond the data.
"""


class AwsBackend:
    """Real Bedrock backend for synthesis.

    Intentionally minimal — we only need converse() for this demo.
    """

    def __init__(self, region: str, model_id: str):
        self._client = boto3.client("bedrock-runtime", region_name=region)
        self._model_id = model_id

    def converse(self, system: str, user_message: str) -> tuple[str, dict]:
        """Call Bedrock converse() and return (text, usage)."""
        resp = self._client.converse(
            modelId=self._model_id,
            system=[{"text": system}],
            messages=[{"role": "user", "content": [{"text": user_message}]}],
            inferenceConfig={"maxTokens": 1024},
        )
        text = resp["output"]["message"]["content"][0]["text"]
        usage = resp.get("usage", {})
        return text, usage


class FakeBackend:
    """Test double — returns canned text, no AWS calls."""

    def converse(self, system: str, user_message: str) -> tuple[str, dict]:  # noqa: ARG002
        return (
            "1. The cohort yields tens of thousands of chr20 variants, dominated "
            "by SNPs over indels at the expected ~9:1 ratio.\n"
            "2. The cohort Ti/Tv ratio sits in the ~2.0-2.1 range typical of "
            "high-quality human WGS, indicating a low false-positive call rate.\n"
            "3. Graviton3 instances ran the bcftools mpileup/call fan-out at "
            "parity with x86 on native arm64 containers — neither emulates.",
            {"inputTokens": 120, "outputTokens": 80},
        )


def synthesize(
    summary: dict,
    emit: Callable[[dict], None],
    backend=None,
) -> None:
    """Run Bedrock synthesis and emit events.

    Args:
        summary:  the pipeline summary dict (from pipeline.read_summary()).
        emit:     event callback compatible with app.py's WebSocket protocol.
        backend:  AwsBackend (or a FakeBackend for tests).  If None, creates
                  a real AwsBackend from the BEDROCK_REGION / BEDROCK_MODEL
                  in config.  Must be provided by caller if running without
                  AWS credentials.
    """
    if backend is None:
        import config as cfg  # type: ignore[import]

        backend = AwsBackend(cfg.BEDROCK_REGION, cfg.BEDROCK_MODEL)

    emit({"type": "phase", "label": "Synthesizing insights with Claude Sonnet…"})
    emit({"type": "model", "tier": "sonnet", "label": "Claude Sonnet", "state": "start"})

    user_message = _build_prompt(summary)
    text, usage = backend.converse(_SYNTHESIS_SYSTEM, user_message)

    in_tok = usage.get("inputTokens", 0)
    out_tok = usage.get("outputTokens", 0)
    cost = (in_tok * _SONNET_INPUT_USD_PER_1K + out_tok * _SONNET_OUTPUT_USD_PER_1K) / 1000.0

    emit(
        {
            "type": "model",
            "tier": "sonnet",
            "label": "Claude Sonnet",
            "state": "done",
            "usage": {"inputTokens": in_tok, "outputTokens": out_tok},
            "cost": cost,
        }
    )

    emit({"type": "insight", "text": text})
    emit({"type": "cost", "total": cost})
    emit({"type": "done"})


def _build_prompt(summary: dict) -> str:
    """Convert the pipeline summary dict into a structured prompt."""
    lines: list[str] = [
        f"Total samples analysed: {summary.get('total_samples', '?')}",
        f"Pipeline elapsed time: {_fmt_elapsed(summary.get('elapsed_seconds', 0))}",
        f"Estimated EC2 cost: ${summary.get('ec2_cost_usd', 0):.4f}",
        "",
        "Samples per super-population:",
    ]

    super_pops = summary.get("super_populations", {})
    for spop, count in super_pops.items():
        lines.append(f"  {spop}: {count} samples")

    stats = summary.get("vcf_stats", {})
    if stats:
        lines.append("")
        lines.append("Cohort VCF statistics (bcftools stats, chr20):")
        lines.append(f"  Total records: {stats.get('total_records', 'N/A')}")
        lines.append(f"  SNPs: {stats.get('snps', 'N/A')}")
        lines.append(f"  Indels: {stats.get('indels', 'N/A')}")
        lines.append(f"  Ti/Tv ratio: {stats.get('ti_tv_ratio', 'N/A')}")

    return "\n".join(lines)


def _fmt_elapsed(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s}s" if m else f"{s}s"
