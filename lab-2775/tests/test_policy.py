"""Tests for pipeline.logic.policy — the broadcast control-plane record."""

import pytest

from pipeline.logic.policy import Policy, PolicyError, parse_policy


def test_defaults_are_safe():
    p = Policy()
    assert p.pii_redact is True
    assert "email" in p.pii_detectors
    assert p.drop_low_quality is False


def test_partial_update_keeps_other_fields():
    base = Policy(min_chars=99)
    updated = parse_policy('{"drop_low_quality": true}', base)
    assert updated.drop_low_quality is True
    assert updated.min_chars == 99


def test_round_trip_through_json():
    p = Policy(min_chars=7, pii_redact=False)
    assert parse_policy(p.to_json()) == p


def test_unknown_key_is_rejected():
    # A typo must not silently do nothing; the operator logs and keeps the old
    # policy rather than applying half an update.
    with pytest.raises(PolicyError, match="unknown policy keys"):
        parse_policy('{"pii_redcat": false}')


def test_unknown_detector_is_rejected():
    with pytest.raises(PolicyError, match="unknown pii detectors"):
        parse_policy('{"pii_detectors": ["email", "telepathy"]}')


def test_malformed_json_is_rejected():
    with pytest.raises(PolicyError, match="not JSON"):
        parse_policy("{not json")


def test_non_object_payload_is_rejected():
    with pytest.raises(PolicyError, match="must be a JSON object"):
        parse_policy("[1, 2, 3]")


@pytest.mark.parametrize(
    "payload",
    ['{"min_chars": -1}', '{"min_chars": "40"}', '{"min_chars": true}'],
)
def test_bad_min_chars_is_rejected(payload):
    with pytest.raises(PolicyError, match="min_chars"):
        parse_policy(payload)


def test_a_bool_is_not_accepted_where_a_number_is_expected():
    """`{"min_chars": true}` is 1 to Python and a mistake to everyone else."""
    with pytest.raises(PolicyError, match="min_chars"):
        parse_policy('{"min_chars": true}')


def test_detectors_and_blocked_doc_ids_become_frozensets():
    p = parse_policy('{"pii_detectors": ["email"], "blocked_doc_ids": ["a.pdf", "b.pdf"]}')
    assert p.pii_detectors == frozenset({"email"})
    assert p.blocked_doc_ids == frozenset({"a.pdf", "b.pdf"})
