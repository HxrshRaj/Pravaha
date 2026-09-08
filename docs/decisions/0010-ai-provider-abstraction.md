# ADR 0010 — AI provider abstraction & grounding

**Status:** accepted

## Decision
A one-method interface (`BaseClient.chat(messages, tools) -> AIResponse`) with three
implementations, tried in `AI_PROVIDER_ORDER`:

1. `OpenAICompatibleClient` — OpenAI API and anything wire-compatible (Groq, vLLM, …),
   selected by configured base URL + key.
2. `MockClient` — fully deterministic, offline, no key. Runs a scripted evidence-collection
   plan against the real tools, then emits a rule-based conclusion derived **only** from
   what those tools returned. Used for the demo and for deterministic evaluation.

### Grounding & safety (not provider-specific)
- The model gets an anomaly + **read-only, allow-listed tools** only — no shell, no writes,
  no Kafka admin.
- Every tool call → `ai_tool_calls`; every provider call's tokens/latency/cost →
  `ai_usage`.
- The final JSON is validated; `evidence_refs` are intersected with ids that tools actually
  returned. Hallucinated refs are dropped and recorded (`hallucinated_refs`,
  `grounding_ratio`); confidence is capped low when nothing is grounded.
- Prompts are versioned (`PROMPT_VERSION`) and stored on each investigation.

## Consequences
- **AI is never on the critical path.** Provider down → try next → else investigation
  `FAILED` with a reason; event processing is untouched (`tests/failure`).
- Swapping providers is a config change; evaluation pins the mock for reproducibility.
- Cost tracking uses a small static price table — figures are estimates.
