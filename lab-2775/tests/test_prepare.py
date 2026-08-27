"""The merged prepare stage, exercised as the operator actually runs it.

``PrepareFunction`` is three stages in one operator, which is a deliberate
trade (see the module docstring) and therefore worth a test that the fusion did
not change the result: the same drops, the same normalization, the same derived
fields, in the order that makes the fingerprint follow the normalized text.

It imports ``pyflink``, so this file only runs inside the pipeline image.
"""

import json

import pytest

pytest.importorskip("pyflink", reason="operator modules import pyflink; run inside the image")

from pipeline.logic.enrichment import fingerprint, normalize_text  # noqa: E402
from pipeline.stages.prepare import PrepareFunction  # noqa: E402


def run(record: dict, min_chars: int = 40, drop_low_quality: bool = False) -> list[dict]:
    """One record through the operator, without a Flink runtime.

    ``open()`` is skipped, so the metric counters stay None — which is the point
    of guarding every ``.inc()``: the operator has to work without a metrics
    group, or this test could not exist.
    """
    fn = PrepareFunction(min_chars, drop_low_quality=drop_low_quality)
    return [json.loads(v) for v in fn.flat_map(json.dumps(record))]


def chunk(text: str, **extra) -> dict:
    return {"doc_id": "paper.pdf", "chunk_index": 0, "text": text, **extra}


def test_a_normal_chunk_comes_out_enriched():
    out = run(chunk("The quick brown fox jumps over the lazy dog, repeatedly and at length."))
    assert len(out) == 1
    rec = out[0]
    assert rec["doc_id"] == "paper.pdf"          # nothing is renamed or moved
    assert rec["char_count"] > 0 and rec["word_count"] > 0
    assert rec["content_type"] == "prose"
    assert rec["keep"] is True
    assert rec["ingested_at"].endswith("Z")


def test_control_records_and_empty_chunks_are_dropped():
    assert run({"doc_id": "d", "kind": "document-summary"}) == []
    assert run(chunk("   ")) == []
    assert run({"text": "no doc_id here"}) == []


def test_the_fingerprint_follows_the_normalized_text():
    """The whole reason normalize runs before enrich: two chunks that differ
    only by a soft hyphen have to dedup against each other."""
    plain = "Cooperation between the two teams was excellent throughout the year."
    fancy = plain.replace("Cooperation", "Co­opera­tion").replace(" ", " ", 1)
    a, b = run(chunk(plain))[0], run(chunk(fancy))[0]
    assert a["fingerprint"] == b["fingerprint"]
    assert a["fingerprint"] == fingerprint(normalize_text(plain))


def test_the_original_text_is_kept_when_normalization_changed_it():
    fancy = "Co­opera­tion between teams was excellent throughout the whole year."
    rec = run(chunk(fancy))[0]
    assert rec["normalized"] is True
    assert rec["raw_text"] == fancy
    assert "­" not in rec["text"]


def test_an_untouched_chunk_carries_no_raw_text():
    rec = run(chunk("Plain sentence with nothing unusual in it at all, honestly."))[0]
    assert rec["normalized"] is False
    assert "raw_text" not in rec


def test_the_quality_verdict_is_computed_here_but_not_acted_on_by_default():
    """The full job leaves the gate off and lets the guard decide, because there
    the threshold is a live policy — and a short chunk has to reach it for the
    rejected topic to ever see it."""
    rec = run(chunk("Too short."), min_chars=200)[0]
    assert rec["keep"] is False
    assert rec["quality_flags"]


def test_the_simple_job_can_drop_on_the_verdict_instead():
    assert run(chunk("Too short."), min_chars=200, drop_low_quality=True) == []
    assert run(chunk("A sentence long enough to be worth keeping around."), drop_low_quality=True)


def test_code_chunks_are_labelled():
    rec = run(chunk("```python\nprint('hello')\n```\nand some words after it."))[0]
    assert rec["content_type"] == "code"
