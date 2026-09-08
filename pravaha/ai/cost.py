"""AI cost tracking.

A small static price table (USD per 1M tokens) covering the default models, with
a conservative fallback. ``estimate_cost`` never raises; unknown models cost 0
for the mock provider and a fallback rate otherwise.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from pravaha.logging import get_logger
from pravaha.observability.metrics import AI_TOKENS_TOTAL

log = get_logger(__name__)

# USD per 1,000,000 tokens: (prompt, completion)
_PRICES: dict[str, tuple[float, float]] = {
    "gpt-4o-mini": (0.15, 0.60),
    "gpt-4o": (2.50, 10.00),
    "gpt-4.1-mini": (0.40, 1.60),
    "llama-3.3-70b-versatile": (0.59, 0.79),
    "llama-3.1-8b-instant": (0.05, 0.08),
    "pravaha-mock-analyst-v1": (0.0, 0.0),
}
_FALLBACK = (1.0, 3.0)


def estimate_cost(model: str, prompt_tokens: int, completion_tokens: int) -> float:
    p_rate, c_rate = _PRICES.get(model, _FALLBACK)
    return round(
        (prompt_tokens / 1_000_000) * p_rate + (completion_tokens / 1_000_000) * c_rate, 8
    )


async def record_usage(
    session: AsyncSession,
    *,
    provider: str,
    model: str,
    operation: str,
    prompt_tokens: int,
    completion_tokens: int,
    latency_ms: int,
    investigation_id: str | None = None,
) -> float:
    from pravaha.models import AIUsage

    total = prompt_tokens + completion_tokens
    cost = estimate_cost(model, prompt_tokens, completion_tokens)
    session.add(
        AIUsage(
            investigation_id=investigation_id,
            provider=provider,
            model=model,
            operation=operation,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total,
            latency_ms=latency_ms,
            estimated_cost_usd=cost,
        )
    )
    AI_TOKENS_TOTAL.labels(provider=provider, model=model, kind="prompt").inc(prompt_tokens)
    AI_TOKENS_TOTAL.labels(provider=provider, model=model, kind="completion").inc(completion_tokens)
    log.info(
        "ai.usage",
        provider=provider,
        model=model,
        operation=operation,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        latency_ms=latency_ms,
        estimated_cost_usd=cost,
        investigation_id=investigation_id,
    )
    return cost
