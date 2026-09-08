"""AI event investigator - the orchestration state machine.

Flow (persisted on the ``ai_investigations`` row):

    CREATED -> RUNNING
      phase: evidence_collection  (model calls read-only tools in a loop)
      phase: hypothesis_generation / hypothesis_testing (model reasons)
      phase: conclusion
    -> COMPLETED   (structured result persisted: summary, root_cause, impact,
                    hypotheses, recommendations, evidence)
    -> FAILED      (provider error / bad output / timeout; reason recorded)
    -> CANCELLED   (operator)

Guarantees / safety:
* Tools are allow-listed and read-only; every call is stored as an AIToolCall.
* Bounded tool-call budget (``AI_MAX_TOOL_CALLS``).
* Every AI provider call's token usage + cost is recorded (``ai_usage``).
* Provider failure never propagates to the streaming platform - it only marks
  this investigation FAILED. Provider fallback order is honoured.
* The model's final JSON is validated; evidence_refs are cross-checked against
  refs actually returned by tools (hallucinated ids are dropped and flagged).
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select

from pravaha.ai.cost import record_usage
from pravaha.ai.prompts import PROMPT_VERSION, SYSTEM_PROMPT, USER_PROMPT_TEMPLATE
from pravaha.ai.provider import (
    AIProviderError,
    AIResponse,
    BaseClient,
    ChatMessage,
    get_ai_client,
)
from pravaha.ai.tools import (
    TOOL_SPECS,
    ToolContext,
    parse_tool_arguments,
    run_tool,
)
from pravaha.config import settings
from pravaha.db import session_scope
from pravaha.logging import get_logger
from pravaha.models import (
    AIEvidence,
    AIHypothesis,
    AIInvestigation,
    AIToolCall,
    AnomalyRecord,
)
from pravaha.observability.metrics import AI_INVESTIGATIONS_TOTAL, AI_LATENCY

log = get_logger(__name__)


class InvestigationError(Exception):
    pass


async def create_investigation(
    anomaly_id: str, *, trigger: str = "anomaly", title: str | None = None
) -> str:
    async with session_scope() as s:
        anomaly = await s.get(AnomalyRecord, anomaly_id)
        if anomaly is None:
            raise InvestigationError(f"anomaly {anomaly_id} not found")
        inv = AIInvestigation(
            anomaly_id=anomaly_id,
            trigger=trigger,
            title=title or f"{anomaly.severity} anomaly on {anomaly.metric}",
            status="CREATED",
            phase="created",
            prompt_version=PROMPT_VERSION,
        )
        s.add(inv)
        if anomaly.status == "OPEN":
            anomaly.status = "INVESTIGATING"
        await s.flush()
        return inv.id


async def run_investigation(investigation_id: str, *, provider: str | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    async with session_scope() as s:
        inv = await s.get(AIInvestigation, investigation_id)
        if inv is None:
            raise InvestigationError("investigation not found")
        if inv.status in ("COMPLETED", "CANCELLED"):
            return {"status": inv.status, "id": inv.id}
        anomaly = await s.get(AnomalyRecord, inv.anomaly_id)
        inv.status = "RUNNING"
        inv.phase = "evidence_collection"
        inv.started_at = datetime.now(UTC)

    client = get_ai_client(provider)
    ctx = ToolContext(anomaly=anomaly, investigation_id=investigation_id)

    messages: list[ChatMessage] = [
        ChatMessage("system", SYSTEM_PROMPT),
        ChatMessage("user", _render_user_prompt(anomaly)),
    ]
    collected_refs: set[str] = set()
    tool_seq = 0

    try:
        final_json = await _agent_loop(
            client, messages, ctx, investigation_id, collected_refs, lambda: tool_seq
        )
    except AIProviderError as exc:
        return await _fail(investigation_id, f"AI provider error: {exc}", started)
    except Exception as exc:  # noqa: BLE001
        log.exception("ai.investigation_crashed", investigation_id=investigation_id)
        return await _fail(investigation_id, f"investigation error: {exc}", started)

    if final_json is None:
        return await _fail(investigation_id, "model did not produce a valid conclusion", started)

    result = _validate_conclusion(final_json, collected_refs)
    await _complete(investigation_id, client, result, started)
    AI_INVESTIGATIONS_TOTAL.labels(result="completed").inc()
    return {"status": "COMPLETED", "id": investigation_id, **result}


async def _agent_loop(
    client: BaseClient,
    messages: list[ChatMessage],
    ctx: ToolContext,
    investigation_id: str,
    collected_refs: set[str],
    _seq,
) -> dict[str, Any] | None:
    seq = 0
    for _step in range(settings.ai_max_tool_calls + 3):
        t0 = time.perf_counter()
        resp: AIResponse = await client.chat(messages, TOOL_SPECS)
        AI_LATENCY.labels(provider=resp.provider, operation="chat").observe(
            time.perf_counter() - t0
        )
        async with session_scope() as s:
            await record_usage(
                s,
                provider=resp.provider,
                model=resp.model,
                operation="chat",
                prompt_tokens=resp.prompt_tokens,
                completion_tokens=resp.completion_tokens,
                latency_ms=resp.latency_ms,
                investigation_id=investigation_id,
            )
            inv = await s.get(AIInvestigation, investigation_id)
            inv.provider, inv.model = resp.provider, resp.model

        if resp.tool_calls:
            if seq >= settings.ai_max_tool_calls:
                messages.append(
                    ChatMessage(
                        "user",
                        "Tool-call budget exhausted. Conclude now with the JSON object.",
                    )
                )
                continue
            messages.append(
                ChatMessage("assistant", resp.text or "", tool_calls=resp.tool_calls)
            )
            for tc in resp.tool_calls:
                seq += 1
                name = tc.get("function", {}).get("name", "")
                args = parse_tool_arguments(tc.get("function", {}).get("arguments"))
                tool_started = time.perf_counter()
                ok, err, payload = True, None, {}
                try:
                    payload = await run_tool(name, ctx, args)
                except Exception as exc:  # noqa: BLE001
                    ok, err, payload = False, str(exc), {"error": str(exc)}
                latency_ms = int((time.perf_counter() - tool_started) * 1000)
                refs = _extract_refs(payload)
                collected_refs.update(refs)
                await _store_tool_call(
                    investigation_id, seq, name, args, payload, ok, err, latency_ms
                )
                if ok:
                    await _store_evidence(investigation_id, name, payload, refs)
                messages.append(
                    ChatMessage(
                        "tool",
                        json.dumps(payload, default=str)[:12000],
                        tool_call_id=tc.get("id", f"call-{seq}"),
                        name=name,
                    )
                )
            await _set_phase(investigation_id, "hypothesis_testing")
            continue

        # no tool calls -> expect the final JSON
        parsed = _extract_json(resp.text)
        if parsed is not None:
            return parsed
        messages.append(
            ChatMessage(
                "user",
                "Please respond with ONLY the JSON conclusion object as specified.",
            )
        )
    return None


def _render_user_prompt(a: AnomalyRecord) -> str:
    return USER_PROMPT_TEMPLATE.format(
        anomaly_id=a.id,
        metric=a.metric,
        group_key=a.group_key,
        detected_at=a.detected_at.isoformat(),
        window_start=a.window_start.isoformat(),
        window_end=a.window_end.isoformat(),
        observed_value=a.observed_value,
        expected_value=a.expected_value,
        deviation=a.deviation,
        severity=a.severity,
        algorithm=a.algorithm,
        detector_evidence=json.dumps(a.evidence)[:1500],
    )


def _extract_refs(payload: Any) -> set[str]:
    out: set[str] = set()

    def walk(x: Any) -> None:
        if isinstance(x, dict):
            for k, v in x.items():
                if k == "ref" and isinstance(v, str):
                    out.add(v)
                elif k == "evidence_refs" and isinstance(v, list):
                    out.update(r for r in v if isinstance(r, str))
                else:
                    walk(v)
        elif isinstance(x, list):
            for i in x:
                walk(i)

    walk(payload)
    return out


def _extract_json(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:]
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end <= start:
        return None
    try:
        obj = json.loads(text[start : end + 1])
        return obj if isinstance(obj, dict) else None
    except json.JSONDecodeError:
        return None


def _validate_conclusion(raw: dict[str, Any], collected_refs: set[str]) -> dict[str, Any]:
    hyps_in = raw.get("hypotheses", []) if isinstance(raw.get("hypotheses"), list) else []
    hypotheses = []
    for h in hyps_in:
        if not isinstance(h, dict):
            continue
        refs = [r for r in h.get("evidence_refs", []) if isinstance(r, str)]
        grounded = [r for r in refs if r in collected_refs]
        hypotheses.append(
            {
                "key": str(h.get("key", f"H{len(hypotheses)+1}"))[:8],
                "statement": str(h.get("statement", ""))[:400],
                "explanation": str(h.get("explanation", ""))[:1200],
                "confidence": _clamp01(h.get("confidence", 0.0)),
                "supporting": [str(x)[:240] for x in h.get("supporting", [])][:8],
                "contradicting": [str(x)[:240] for x in h.get("contradicting", [])][:8],
                "evidence_refs": grounded,
                "hallucinated_refs": [r for r in refs if r not in collected_refs],
            }
        )

    all_refs = [r for r in raw.get("evidence_refs", []) if isinstance(r, str)]
    grounded_refs = [r for r in all_refs if r in collected_refs]
    hallucinated = [r for r in all_refs if r not in collected_refs]

    conf = _clamp01(raw.get("confidence", 0.0))
    root_cause = str(raw.get("root_cause", "inconclusive"))[:300]
    if not grounded_refs and root_cause != "inconclusive":
        # No grounded evidence at all -> downgrade honesty
        conf = min(conf, 0.2)

    return {
        "summary": str(raw.get("summary", ""))[:2000],
        "root_cause": root_cause,
        "impact": str(raw.get("impact", ""))[:1000],
        "confidence": conf,
        "hypotheses": hypotheses or [
            {
                "key": "H0",
                "statement": "insufficient structured hypotheses returned",
                "confidence": 0.0,
                "supporting": [],
                "contradicting": [],
                "evidence_refs": [],
                "hallucinated_refs": [],
            }
        ],
        "recommendations": [str(x)[:300] for x in raw.get("recommendations", [])][:10],
        "evidence_refs": grounded_refs,
        "hallucinated_refs": hallucinated,
        "grounding_ratio": round(
            len(grounded_refs) / len(all_refs), 3
        ) if all_refs else 0.0,
    }


async def _store_tool_call(inv_id, seq, name, args, payload, ok, err, latency_ms) -> None:  # noqa: ANN001
    async with session_scope() as s:
        s.add(
            AIToolCall(
                investigation_id=inv_id,
                seq=seq,
                tool_name=name,
                arguments=args,
                result=_truncate_json(payload),
                result_summary=_summarise(payload),
                ok=ok,
                error=err,
                latency_ms=latency_ms,
            )
        )


async def _store_evidence(inv_id, source_tool, payload, refs) -> None:  # noqa: ANN001
    if not refs:
        return
    async with session_scope() as s:
        existing = set(
            await s.scalars(
                select(AIEvidence.ref).where(AIEvidence.investigation_id == inv_id)
            )
        )
        for ref in list(refs)[:40]:
            if ref in existing:
                continue
            s.add(
                AIEvidence(
                    investigation_id=inv_id,
                    ref=ref[:48],
                    kind=ref.split(":", 1)[0],
                    source_tool=source_tool,
                    summary=_summarise(payload)[:500],
                    data={},
                )
            )


async def _set_phase(inv_id: str, phase: str) -> None:
    async with session_scope() as s:
        inv = await s.get(AIInvestigation, inv_id)
        if inv and inv.status == "RUNNING":
            inv.phase = phase


async def _complete(inv_id: str, client: BaseClient, result: dict[str, Any], started: float) -> None:
    async with session_scope() as s:
        inv = await s.get(AIInvestigation, inv_id)
        inv.status = "COMPLETED"
        inv.phase = "conclusion"
        inv.summary = result["summary"]
        inv.root_cause = result["root_cause"]
        inv.impact = result["impact"]
        inv.recommendations = result["recommendations"]
        inv.conclusion_confidence = result["confidence"]
        inv.finished_at = datetime.now(UTC)
        inv.duration_ms = int((time.perf_counter() - started) * 1000)
        for h in result["hypotheses"]:
            s.add(
                AIHypothesis(
                    investigation_id=inv_id,
                    key=h["key"],
                    statement=h["statement"],
                    explanation=h.get("explanation", ""),
                    confidence=h["confidence"],
                    status="SELECTED"
                    if h["statement"] and result["root_cause"] in h["statement"]
                    else "PROPOSED",
                    supporting=h["supporting"],
                    contradicting=h["contradicting"],
                    evidence_refs=h["evidence_refs"],
                )
            )
        anomaly = await s.get(AnomalyRecord, inv.anomaly_id)
        if anomaly and anomaly.status == "INVESTIGATING":
            anomaly.notes = (result["summary"] or "")[:2000]
    log.info(
        "ai.investigation_completed",
        investigation_id=inv_id,
        root_cause=result["root_cause"],
        confidence=result["confidence"],
        grounding_ratio=result["grounding_ratio"],
        hallucinated=len(result["hallucinated_refs"]),
    )


async def _fail(inv_id: str, reason: str, started: float) -> dict[str, Any]:
    AI_INVESTIGATIONS_TOTAL.labels(result="failed").inc()
    try:
        async with session_scope() as s:
            inv = await s.get(AIInvestigation, inv_id)
            if inv is not None:
                inv.status = "FAILED"
                inv.error = reason[:4000]
                inv.finished_at = datetime.now(UTC)
                inv.duration_ms = int((time.perf_counter() - started) * 1000)
                anomaly = await s.get(AnomalyRecord, inv.anomaly_id)
                if anomaly and anomaly.status == "INVESTIGATING":
                    anomaly.status = "OPEN"
    except Exception as exc:  # noqa: BLE001 - AI failure path must never raise
        log.error("ai.fail_persist_failed", investigation_id=inv_id, error=str(exc))
    log.warning("ai.investigation_failed", investigation_id=inv_id, reason=reason)
    return {"status": "FAILED", "id": inv_id, "error": reason}


def _summarise(payload: Any) -> str:
    if not isinstance(payload, dict):
        return str(payload)[:200]
    if "error" in payload:
        return f"error: {payload['error']}"
    keys = [k for k in payload if k != "evidence_refs"]
    bits = []
    for k in keys[:4]:
        v = payload[k]
        if isinstance(v, (list, dict)):
            bits.append(f"{k}={type(v).__name__}({len(v)})")
        else:
            bits.append(f"{k}={v}")
    return ", ".join(bits)[:400]


def _truncate_json(payload: Any, limit: int = 8000) -> Any:
    s = json.dumps(payload, default=str)
    if len(s) <= limit:
        return payload
    return {"_truncated": True, "preview": s[:limit]}


def _clamp01(v: Any) -> float:
    try:
        return max(0.0, min(1.0, float(v)))
    except (TypeError, ValueError):
        return 0.0
