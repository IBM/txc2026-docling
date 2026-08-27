"""Tests for pipeline.logic.pii — detection, checksum validation and redaction."""

from pipeline.logic.pii import DEFAULT_ENABLED, detect, redact, summarize


def test_email_is_detected_and_replaced():
    out, findings = redact("write to jane.doe@acme.com please")
    assert out == "write to [REDACTED:EMAIL] please"
    assert summarize(findings) == {"email": 1}


def test_valid_credit_card_is_redacted():
    out, findings = redact("card 4111 1111 1111 1111 on file")
    assert "4111" not in out
    assert summarize(findings) == {"credit_card": 1}


def test_digit_run_failing_luhn_is_left_alone():
    # The whole point of the checksum: a table of 16-digit ids is not PII.
    text = "reference 9999 8888 7777 6666 in the table"
    out, findings = redact(text)
    assert out == text
    assert findings == []


def test_valid_iban_is_redacted_and_invalid_one_is_not():
    out, findings = redact("pay DE89 3704 0044 0532 0130 00 not DE00 1111 2222 3333 4444 55")
    assert "DE89" not in out
    assert "DE00 1111 2222 3333 4444 55" in out
    assert summarize(findings) == {"iban": 1}


def test_compact_iban_without_spaces():
    _, findings = redact("DE89370400440532013000")
    assert summarize(findings) == {"iban": 1}


def test_national_ids_are_not_detected():
    """No detector for these, deliberately.

    A dashed US SSN was the only national identifier the guard ever matched,
    and matching one country's format was more misleading than matching none:
    it made the guard look like it understood national ids. It has no
    checksum to validate against either, unlike the card and the IBAN.
    """
    for text in ("ssn 123-45-6789", "AHV 756.1234.5678.97", "codice fiscale RSSMRA81C03A123X"):
        assert redact(text) == (text, [])


def test_phone_is_off_by_default_but_can_be_enabled():
    text = "call +41 44 123 45 67 now"
    assert redact(text)[1] == []
    _, findings = redact(text, DEFAULT_ENABLED | {"phone"})
    assert summarize(findings) == {"phone": 1}


def test_disabled_detector_finds_nothing():
    assert redact("jane@acme.com", frozenset()) == ("jane@acme.com", [])


def test_clean_text_is_returned_unchanged_and_identical():
    text = "ordinary prose about quarterly finance results"
    out, findings = redact(text)
    assert out is text or out == text
    assert findings == []


def test_input_is_never_mutated():
    text = "mail jane@acme.com"
    original = str(text)
    redact(text)
    assert text == original


def test_findings_are_sorted_and_non_overlapping():
    text = "a@b.com then 4111 1111 1111 1111 then c@d.com"
    findings = detect(text)
    starts = [f["start"] for f in findings]
    assert starts == sorted(starts)
    for earlier, later in zip(findings, findings[1:]):
        assert earlier["end"] <= later["start"]


def test_multiple_types_in_one_chunk():
    out, findings = redact("jane@acme.com paid with 4111 1111 1111 1111 from DE89 3704 0044 0532 0130 00")
    assert summarize(findings) == {"email": 1, "credit_card": 1, "iban": 1}
    assert "REDACTED:EMAIL" in out and "REDACTED:CREDIT_CARD" in out and "REDACTED:IBAN" in out


def test_redaction_preserves_surrounding_text_exactly():
    out, _ = redact("start jane@acme.com end")
    assert out.startswith("start ") and out.endswith(" end")


def test_empty_text():
    assert redact("") == ("", [])
    assert detect("") == []
