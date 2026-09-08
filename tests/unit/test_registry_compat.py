from pravaha.registry.service import check_backward_compatible, validate_payload

BASE = {
    "type": "object",
    "required": ["order_id", "amount"],
    "properties": {
        "order_id": {"type": "string"},
        "amount": {"type": "number"},
        "note": {"type": "string"},
    },
    "additionalProperties": True,
}


def test_validate_payload_reports_paths():
    errs = validate_payload({"amount": "ten"}, BASE)
    assert any("order_id" in e for e in errs)
    assert any("amount" in e for e in errs)
    assert validate_payload({"order_id": "o1", "amount": 10}, BASE) == []


def test_backward_compatible_add_optional_field_ok():
    new = {
        **BASE,
        "properties": {**BASE["properties"], "currency": {"type": "string"}},
    }
    ok, problems = check_backward_compatible(BASE, new)
    assert ok, problems


def test_backward_incompatible_new_required_field():
    new = {**BASE, "required": ["order_id", "amount", "currency"]}
    ok, problems = check_backward_compatible(BASE, new)
    assert not ok
    assert any("required" in p for p in problems)


def test_backward_incompatible_type_narrowing():
    new = {
        **BASE,
        "properties": {**BASE["properties"], "amount": {"type": "integer"}},
    }
    ok, problems = check_backward_compatible(BASE, new)
    assert not ok
    assert any("amount" in p for p in problems)


def test_backward_incompatible_tighten_additional_properties():
    new = {**BASE, "additionalProperties": False}
    ok, problems = check_backward_compatible(BASE, new)
    assert not ok
