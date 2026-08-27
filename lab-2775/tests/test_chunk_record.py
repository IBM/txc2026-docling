"""The chunk record — the pipeline's input contract.

The cases here are the ones that actually occur on the topic: chunks written
before the structure fields were switched on (no headings, no pages), chunks
written after, and the locally-produced records that have to be
indistinguishable from the converter's.
"""

from __future__ import annotations

import hashlib

from pipeline.logic import chunk_record as cr
from pipeline.logic.enrichment import enrich

# A verbatim message from docling.chunks, produced with the target defaults.
MINIMAL = {
    "text": "Abstract\nWe present Deep Search DocQA.",
    "metadata": {
        "origin": {
            "mimetype": "application/pdf",
            "binary_hash": 13823681836276983029,
            "filename": "2311.18481v1.pdf",
            "uri": None,
        },
        "has_image": False,
    },
    "doc_id": "input_documents/2311.18481v1.pdf",
    "chunk_index": 1,
    "chunk_id": "66908c300159e211b7e4840e842bf32d23fe5444aaec2be5f2611dfd40592799",
}

# The same message when the producer set headings_field / page_field.
RICH = dict(MINIMAL, headings=["Introduction", "Motivation"], page_numbers=[4, 3])


def test_accessors_on_the_converter_record():
    assert cr.doc_id(MINIMAL) == "input_documents/2311.18481v1.pdf"
    assert cr.chunk_index(MINIMAL) == 1
    assert cr.filename(MINIMAL) == "2311.18481v1.pdf"
    assert cr.binary_hash(MINIMAL) == "13823681836276983029"
    assert cr.text(MINIMAL).startswith("Abstract")


def test_missing_structure_is_empty_not_an_error():
    """headings/page_numbers are opt-in on the target, so chunks arrive without."""
    assert cr.headings(MINIMAL) == []
    assert cr.page_numbers(MINIMAL) == []


def test_structure_is_read_and_pages_sorted():
    assert cr.headings(RICH) == ["Introduction", "Motivation"]
    assert cr.page_numbers(RICH) == [3, 4]


def test_control_records_are_not_chunks():
    assert cr.is_chunk(MINIMAL)
    assert not cr.is_chunk({"kind": "doc-summary", "doc_id": "a.pdf"})
    assert not cr.is_chunk({"text": "orphan"})


def test_chunk_id_matches_what_the_service_computed():
    """Verified against a record the Docling target actually wrote: the id is
    sha256("<sha256 of the file bytes>:<doc_id>:<chunk_index>")."""
    file_hash = "0e894891f47af5d7646b03cdebe391303c16d328c35227dbbfd795639b4f24f5"
    assert (
        cr.stable_chunk_id(file_hash, "2311.18481", 0)
        == "0ace8cb8137738a7bec94578e53a1f2c4f6fdcc80683448400b64fd96e45ed08"
    )


def test_document_hash_is_the_plain_file_digest():
    assert cr.document_hash(b"hello") == hashlib.sha256(b"hello").hexdigest()


def test_build_produces_the_wire_format():
    """A locally-converted file must be indistinguishable from a converted one."""
    record = cr.build(
        doc_id="2311.18481",
        chunk_index=0,
        text="x",
        document_hash="0e894891f47af5d7646b03cdebe391303c16d328c35227dbbfd795639b4f24f5",
        origin={"mimetype": "application/pdf", "filename": "2311.18481", "binary_hash": 1, "uri": None},
        headings=["Intro"],
        page_numbers=[1],
    )
    assert set(record) == {"text", "metadata", "doc_id", "chunk_index", "headings", "page_numbers", "chunk_id"}
    assert record["chunk_id"] == "0ace8cb8137738a7bec94578e53a1f2c4f6fdcc80683448400b64fd96e45ed08"
    assert record["metadata"]["origin"]["filename"] == "2311.18481"


def test_enrichment_adds_fields_without_renaming_any():
    enriched = enrich(MINIMAL)
    for key, value in MINIMAL.items():
        assert enriched[key] == value, f"{key} was modified"
    assert "source" not in enriched
    assert enriched["fingerprint"]
    assert enriched["char_count"] > 0
    assert enriched["token_count"] > 0
    assert enriched["script"] == "latin"


def test_enrichment_derives_an_id_only_when_one_is_missing():
    without = {k: v for k, v in MINIMAL.items() if k != "chunk_id"}
    assert len(enrich(without)["chunk_id"]) == 64
