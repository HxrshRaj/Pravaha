"""The mock AI client + conclusion validation must be deterministic and must
never invent evidence ids that no tool returned."""


from pravaha.ai.investigator import _validate_conclusion
from pravaha.ai.provider import ChatMessage, MockClient, _mock_reason


def test_mock_client_requests_tools_then_concludes():
    client = MockClient()
    from pravaha.ai.tools import TOOL_SPECS

    msgs = [ChatMessage("system", "s"), ChatMessage("user", "investigate")]
    # first call -> a tool call
    r1 = pump(client, msgs, TOOL_SPECS)
    assert r1.finish_reason == "tool_calls"
    assert r1.tool_calls[0]["function"]["name"] == "get_anomaly_context"


def pump(client, msgs, tools):
    import asyncio

    return asyncio.get_event_loop().run_until_complete(client.chat(msgs, tools))


def test_mock_reason_selects_payment_hypothesis_from_real_evidence():
    evidence = {
        "get_anomaly_context": {
            "anomaly": {
                "metric": "payment_failure_rate",
                "observed_value": 0.55,
                "expected_value": 0.07,
            },
            "evidence_refs": ["A:a1", "M:payment_failure_rate@1"],
        },
        "query_metrics": {"latest": {"payment_failure_rate": 0.55, "events_per_sec": 40}, "baseline": {"events_per_sec": 38}},
        "get_consumer_lag": {"series": [{"lag": 12}]},
        "get_data_quality": {"producers": [{"overall_score": 0.98}]},
        "get_correlated_events": {"chains": []},
    }
    out = _mock_reason(evidence)
    assert out["root_cause"] != "inconclusive"
    assert "payment" in out["root_cause"].lower()
    assert out["confidence"] >= 0.6
    assert "A:a1" in out["evidence_refs"]


def test_validate_conclusion_drops_hallucinated_refs():
    raw = {
        "summary": "x",
        "root_cause": "payment degradation",
        "impact": "y",
        "confidence": 0.9,
        "hypotheses": [
            {
                "key": "H1",
                "statement": "payment degradation",
                "confidence": 0.9,
                "supporting": ["s"],
                "contradicting": [],
                "evidence_refs": ["E:real-1", "E:made-up-999"],
            }
        ],
        "recommendations": ["do x"],
        "evidence_refs": ["E:real-1", "M:real-2", "E:hallucinated"],
    }
    collected = {"E:real-1", "M:real-2"}
    result = _validate_conclusion(raw, collected)
    assert result["evidence_refs"] == ["E:real-1", "M:real-2"]
    assert result["hallucinated_refs"] == ["E:hallucinated"]
    assert result["hypotheses"][0]["evidence_refs"] == ["E:real-1"]
    assert result["hypotheses"][0]["hallucinated_refs"] == ["E:made-up-999"]
    assert 0 < result["grounding_ratio"] <= 1


def test_validate_conclusion_downgrades_when_no_grounded_evidence():
    raw = {
        "summary": "x",
        "root_cause": "definitely payments",
        "impact": "y",
        "confidence": 0.95,
        "hypotheses": [],
        "recommendations": [],
        "evidence_refs": ["E:not-real"],
    }
    result = _validate_conclusion(raw, set())
    assert result["confidence"] <= 0.2
