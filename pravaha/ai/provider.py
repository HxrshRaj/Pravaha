"""AI provider abstraction.

One tiny interface -- ``chat(messages, tools)`` returning an :class:`AIResponse`
(assistant text and/or tool calls, plus token usage) -- implemented by:

* :class:`OpenAICompatibleClient` - works against the OpenAI API and anything
  wire-compatible (Groq, together, local vLLM, ...). Selected per configured
  base URL + key.
* :class:`MockClient` - fully deterministic, needs no network or key. It runs a
  scripted investigation: it will call the evidence tools it is offered and then
  emit a structured JSON conclusion derived from what those tools returned. This
  keeps the "AI demo" runnable offline and makes evaluation deterministic, while
  never fabricating evidence (it only references tool results it actually got).

Provider order comes from ``AI_PROVIDER_ORDER``; :func:`get_ai_client` returns
the first usable one. If a real provider raises mid-investigation the caller
falls back to the next, and ultimately the investigation is marked FAILED -- the
streaming platform is never blocked on AI.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from pravaha.config import settings
from pravaha.logging import get_logger

log = get_logger(__name__)


class AIProviderError(Exception):
    pass


@dataclass
class ChatMessage:
    role: str  # system | user | assistant | tool
    content: str
    tool_calls: list[dict[str, Any]] | None = None
    tool_call_id: str | None = None
    name: str | None = None

    def to_wire(self) -> dict[str, Any]:
        d: dict[str, Any] = {"role": self.role, "content": self.content}
        if self.tool_calls:
            d["tool_calls"] = self.tool_calls
        if self.tool_call_id:
            d["tool_call_id"] = self.tool_call_id
        if self.name:
            d["name"] = self.name
        return d


@dataclass
class ToolSpec:
    name: str
    description: str
    parameters: dict[str, Any]

    def to_openai(self) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }


@dataclass
class AIResponse:
    text: str
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    provider: str = ""
    model: str = ""
    latency_ms: int = 0
    finish_reason: str = "stop"


class BaseClient:
    provider = "base"
    model = ""

    async def chat(
        self,
        messages: list[ChatMessage],
        tools: list[ToolSpec] | None = None,
        *,
        temperature: float = 0.1,
        max_tokens: int = 1200,
    ) -> AIResponse:
        raise NotImplementedError

    async def healthy(self) -> bool:
        return True


class OpenAICompatibleClient(BaseClient):
    def __init__(self, provider: str, base_url: str, api_key: str, model: str) -> None:
        self.provider = provider
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model

    async def chat(
        self,
        messages: list[ChatMessage],
        tools: list[ToolSpec] | None = None,
        *,
        temperature: float = 0.1,
        max_tokens: int = 1200,
    ) -> AIResponse:
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [m.to_wire() for m in messages],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if tools:
            payload["tools"] = [t.to_openai() for t in tools]
            payload["tool_choice"] = "auto"

        started = time.perf_counter()
        try:
            async with httpx.AsyncClient(timeout=settings.ai_request_timeout_seconds) as client:
                resp = await client.post(
                    f"{self.base_url}/chat/completions",
                    headers={"Authorization": f"Bearer {self.api_key}"},
                    json=payload,
                )
        except httpx.HTTPError as exc:
            raise AIProviderError(f"{self.provider} request failed: {exc}") from exc
        latency_ms = int((time.perf_counter() - started) * 1000)

        if resp.status_code >= 400:
            raise AIProviderError(
                f"{self.provider} returned {resp.status_code}: {resp.text[:500]}"
            )
        data = resp.json()
        choice = data["choices"][0]
        msg = choice["message"]
        usage = data.get("usage", {})
        return AIResponse(
            text=msg.get("content") or "",
            tool_calls=msg.get("tool_calls") or [],
            prompt_tokens=usage.get("prompt_tokens", 0),
            completion_tokens=usage.get("completion_tokens", 0),
            total_tokens=usage.get("total_tokens", 0),
            provider=self.provider,
            model=self.model,
            latency_ms=latency_ms,
            finish_reason=choice.get("finish_reason", "stop"),
        )

    async def healthy(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                r = await client.get(
                    f"{self.base_url}/models",
                    headers={"Authorization": f"Bearer {self.api_key}"},
                )
            return r.status_code < 500
        except httpx.HTTPError:
            return False


class MockClient(BaseClient):
    """Deterministic, offline. Drives a scripted evidence-grounded investigation."""

    provider = "mock"
    model = "pravaha-mock-analyst-v1"

    def __init__(self) -> None:
        self._step = 0

    async def chat(
        self,
        messages: list[ChatMessage],
        tools: list[ToolSpec] | None = None,
        *,
        temperature: float = 0.1,
        max_tokens: int = 1200,
    ) -> AIResponse:
        started = time.perf_counter()
        tool_names = [t.name for t in (tools or [])]
        called = {
            m.name
            for m in messages
            if m.role == "tool" and m.name
        }
        # Decide the next tool to call: walk a fixed evidence-collection plan.
        plan = [
            ("get_anomaly_context", {}),
            ("query_metrics", {"lookback_minutes": 30}),
            ("get_correlated_events", {"limit": 20}),
            ("get_consumer_lag", {"lookback_minutes": 15}),
            ("get_data_quality", {"lookback_minutes": 30}),
            ("get_recent_alerts", {"lookback_minutes": 30}),
        ]
        pending = [
            (name, args)
            for name, args in plan
            if name in tool_names and name not in called
        ]
        prompt_tokens = sum(len(m.content) // 4 for m in messages)

        if pending:
            name, args = pending[0]
            call_id = f"mock-call-{self._step}"
            self._step += 1
            resp = AIResponse(
                text="",
                tool_calls=[
                    {
                        "id": call_id,
                        "type": "function",
                        "function": {"name": name, "arguments": json.dumps(args)},
                    }
                ],
                prompt_tokens=prompt_tokens,
                completion_tokens=12,
                total_tokens=prompt_tokens + 12,
                provider=self.provider,
                model=self.model,
                latency_ms=int((time.perf_counter() - started) * 1000),
                finish_reason="tool_calls",
            )
            return resp

        # All evidence gathered -> synthesise a grounded conclusion.
        evidence = self._collect_evidence(messages)
        conclusion = _mock_reason(evidence)
        text = json.dumps(conclusion)
        completion_tokens = len(text) // 4
        return AIResponse(
            text=text,
            tool_calls=[],
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=prompt_tokens + completion_tokens,
            provider=self.provider,
            model=self.model,
            latency_ms=int((time.perf_counter() - started) * 1000),
            finish_reason="stop",
        )

    @staticmethod
    def _collect_evidence(messages: list[ChatMessage]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for m in messages:
            if m.role == "tool" and m.name:
                try:
                    out[m.name] = json.loads(m.content)
                except json.JSONDecodeError:
                    out[m.name] = {"raw": m.content}
        return out


def _mock_reason(evidence: dict[str, Any]) -> dict[str, Any]:
    """Rule-based root-cause selection over *real* collected evidence."""
    ctx = evidence.get("get_anomaly_context", {})
    metric = (ctx.get("anomaly", {}) or {}).get("metric", "unknown")
    observed = (ctx.get("anomaly", {}) or {}).get("observed_value")
    expected = (ctx.get("anomaly", {}) or {}).get("expected_value")
    refs = list(ctx.get("evidence_refs", []))

    lag = evidence.get("get_consumer_lag", {})
    dq = evidence.get("get_data_quality", {})
    metrics = evidence.get("query_metrics", {})
    corr = evidence.get("get_correlated_events", {})

    max_lag = 0
    for row in (lag.get("series") or []):
        max_lag = max(max_lag, int(row.get("lag", 0)))
    worst_dq = 1.0
    for row in (dq.get("producers") or []):
        worst_dq = min(worst_dq, float(row.get("overall_score", 1.0)))

    hypotheses = []
    payment_failure = float((metrics.get("latest") or {}).get("payment_failure_rate", 0.0))
    events_ps = float((metrics.get("latest") or {}).get("events_per_sec", 0.0))
    events_ps_prev = float((metrics.get("baseline") or {}).get("events_per_sec", events_ps or 1.0))
    surge_ratio = events_ps / events_ps_prev if events_ps_prev else 1.0

    def h(key, statement, conf, support, contra, ev):
        return {
            "key": key,
            "statement": statement,
            "confidence": round(conf, 2),
            "supporting": support,
            "contradicting": contra,
            "evidence_refs": ev,
        }

    hypotheses.append(
        h(
            "H1",
            "Payment provider / payment-service degradation",
            0.8 if (payment_failure > 0.15 or "payment" in metric) else 0.2,
            [f"payment_failure_rate={payment_failure:.2%}"] if payment_failure else [],
            [] if payment_failure > 0.1 else ["failure rate not elevated"],
            [r for r in refs if r.startswith(("E:", "M:"))][:5],
        )
    )
    hypotheses.append(
        h(
            "H2",
            "Organic traffic surge (load increase, not application failure)",
            0.75 if surge_ratio > 2.5 else 0.15,
            [f"events/sec x{surge_ratio:.1f} vs baseline"] if surge_ratio > 1.5 else [],
            ["error rates flat"] if payment_failure < 0.1 else ["errors also elevated"],
            [r for r in refs if r.startswith("M:")][:3],
        )
    )
    hypotheses.append(
        h(
            "H3",
            "Consumer processing lag / bottleneck",
            0.8 if max_lag > 1000 else 0.1,
            [f"peak consumer lag={max_lag} records"] if max_lag else [],
            ["lag within normal range"] if max_lag <= 1000 else [],
            [r for r in refs if r.startswith("L:")][:3],
        )
    )
    hypotheses.append(
        h(
            "H4",
            "Data-quality degradation from a producer",
            0.78 if worst_dq < 0.7 else 0.1,
            [f"worst producer DQ score={worst_dq:.2f}"] if worst_dq < 0.9 else [],
            ["all producers above 0.9 DQ"] if worst_dq >= 0.9 else [],
            [r for r in refs if r.startswith("D:")][:3],
        )
    )
    hypotheses.append(
        h(
            "H5",
            "Downstream dependency failure (inventory/shipping)",
            0.3,
            [],
            ["no correlated downstream error events"] if not corr.get("chains") else [],
            [r for r in refs if r.startswith("E:")][:3],
        )
    )

    selected = max(hypotheses, key=lambda x: x["confidence"])
    strong = selected["confidence"] >= 0.6
    conf = selected["confidence"] if strong else max(h["confidence"] for h in hypotheses)

    summary = (
        f"Anomaly on '{metric}': observed {observed}, expected {expected}. "
        + (
            f"Evidence most strongly supports: {selected['statement']} "
            f"(confidence {selected['confidence']:.0%})."
            if strong
            else "Evidence is not conclusive; multiple hypotheses remain plausible."
        )
    )
    return {
        "summary": summary,
        "root_cause": selected["statement"] if strong else "inconclusive",
        "impact": _impact_text(metric, observed, expected),
        "confidence": round(conf, 2),
        "hypotheses": hypotheses,
        "recommendations": _recommendations(selected["key"] if strong else None),
        "evidence_refs": refs,
    }


def _impact_text(metric: str, observed: Any, expected: Any) -> str:
    try:
        delta = float(observed) - float(expected)
        return f"{metric} deviated by {delta:+.3f} from the expected baseline for the affected window."
    except (TypeError, ValueError):
        return f"{metric} is outside its expected range for the affected window."


def _recommendations(key: str | None) -> list[str]:
    table = {
        "H1": [
            "Check payment provider status page and recent deploys to payment-service.",
            "Inspect top_payment_failure_reasons for the window; correlate with a provider or BIN range.",
            "If provider-side, enable the fallback processor and alert the payments on-call.",
        ],
        "H2": [
            "Confirm capacity headroom (consumer lag, DB connections) can absorb sustained load.",
            "Scale analytics/persistence consumers horizontally if lag is trending up.",
            "No application rollback needed if error rates are flat.",
        ],
        "H3": [
            "Scale out the lagging consumer group; check for a slow handler or DB contention.",
            "Verify backpressure is engaging (consumer_paused metric) rather than OOM risk.",
            "Replay any DLQ'd events after the bottleneck clears.",
        ],
        "H4": [
            "Identify the producer with the lowest Data Quality Score and inspect its recent payloads.",
            "Tighten or roll back the offending schema version; notify the producing team.",
            "Backfill via replay once the producer is fixed.",
        ],
        "H5": [
            "Trace correlated event chains for stalled transitions (e.g. order.confirmed with no shipment.created).",
            "Check the downstream service's health and queue depth.",
        ],
        None: [
            "Collect one more window of data and re-run the investigation.",
            "Manually inspect the correlated event chains and consumer lag series.",
        ],
    }
    return table.get(key, table[None])


# --------------------------------------------------------------------------- factory


def _build_client(name: str) -> BaseClient | None:
    if name == "mock":
        return MockClient()
    if name == "openai" and settings.ai_openai_api_key:
        return OpenAICompatibleClient(
            "openai", settings.ai_openai_base_url, settings.ai_openai_api_key, settings.ai_openai_model
        )
    if name == "groq" and settings.ai_groq_api_key:
        return OpenAICompatibleClient(
            "groq", settings.ai_groq_base_url, settings.ai_groq_api_key, settings.ai_groq_model
        )
    return None


def get_ai_client(preferred: str | None = None) -> BaseClient:
    order = [preferred] if preferred else []
    order += settings.ai_providers
    for name in order:
        if not name:
            continue
        client = _build_client(name)
        if client is not None:
            return client
    log.warning("ai.no_provider_configured_falling_back_to_mock")
    return MockClient()


def available_providers() -> list[str]:
    return [p for p in settings.ai_providers if _build_client(p) is not None]
