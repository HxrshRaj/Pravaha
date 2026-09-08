from pravaha.processing.pipeline_spec import validate_graph

GOOD = {
    "nodes": [
        {"key": "src", "type": "source", "config": {"topic": "events.validated"}},
        {"key": "flt", "type": "filter", "config": {"clauses": []}},
        {"key": "win", "type": "window", "config": {"window_type": "tumbling", "size": "1m"}},
        {"key": "agg", "type": "aggregate", "config": {"kind": "count"}},
        {"key": "snk", "type": "sink", "config": {}},
    ],
    "edges": [
        {"from": "src", "to": "flt"},
        {"from": "flt", "to": "win"},
        {"from": "win", "to": "agg"},
        {"from": "agg", "to": "snk"},
    ],
}


def test_valid_pipeline():
    r = validate_graph(GOOD)
    assert r.ok, r.errors
    assert r.topological_order[0] == "src"
    assert r.topological_order[-1] == "snk"


def test_cycle_detected():
    g = {
        "nodes": GOOD["nodes"],
        "edges": GOOD["edges"] + [{"from": "snk", "to": "src"}],
    }
    r = validate_graph(g)
    assert not r.ok
    assert any("cycle" in e or "ordering" in e for e in r.errors)


def test_requires_single_source_and_sink():
    g = {"nodes": [{"key": "a", "type": "filter", "config": {}}], "edges": []}
    r = validate_graph(g)
    assert not r.ok
    assert any("source" in e for e in r.errors)
    assert any("sink" in e for e in r.errors)


def test_bad_window_and_aggregate_config():
    g = {
        "nodes": [
            {"key": "src", "type": "source", "config": {}},
            {"key": "win", "type": "window", "config": {"size": "5x"}},
            {"key": "agg", "type": "aggregate", "config": {"kind": "bogus"}},
            {"key": "snk", "type": "sink", "config": {}},
        ],
        "edges": [
            {"from": "src", "to": "win"},
            {"from": "win", "to": "agg"},
            {"from": "agg", "to": "snk"},
        ],
    }
    r = validate_graph(g)
    assert not r.ok
    assert any("window" in e for e in r.errors)
    assert any("aggregate" in e for e in r.errors)


def test_stage_ordering_violation():
    g = {
        "nodes": [
            {"key": "src", "type": "source", "config": {}},
            {"key": "agg", "type": "aggregate", "config": {"kind": "count"}},
            {"key": "flt", "type": "filter", "config": {}},
            {"key": "snk", "type": "sink", "config": {}},
        ],
        "edges": [
            {"from": "src", "to": "agg"},
            {"from": "agg", "to": "flt"},
            {"from": "flt", "to": "snk"},
        ],
    }
    r = validate_graph(g)
    assert not r.ok
    assert any("ordering" in e for e in r.errors)
