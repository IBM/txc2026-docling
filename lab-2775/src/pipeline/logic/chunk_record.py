"""The chunk record — the pipeline's input contract.

Docling produces the chunk topic (``kafka_chunks`` target), so the record on it
is the converter's, and this repo treats it as **the** record rather than
translating it into something of its own::

    {"text": "I. INTRODUCTION\\nOver the past decade, ...",
     "metadata": {"origin": {"mimetype": "application/pdf",
                             "filename": "2206.00785.pdf",
                             "binary_hash": 13823681836276983029,
                             "uri": null},
                  "has_image": false},
     "doc_id": "2206.00785",
     "chunk_index": 1,
     "chunk_id": "49eb...",          # 64 hex, deterministic — see stable_chunk_id
     "headings": ["I. INTRODUCTION"], # only if the producer asked for them
     "page_numbers": [1]}             # likewise

The stages add their derived fields alongside these; nothing is renamed on the
way in. Two things follow from the shape, and both are why this module exists
rather than every stage reaching into the dict itself:

* ``headings`` and ``page_numbers`` are **opt-in** on the Docling target
  (``headings_field`` / ``page_field``), so a chunk may legitimately arrive with
  no structure at all. The accessors here return empty lists, never ``None``.
* the document's identity lives in two places — ``doc_id`` for keying, and
  ``metadata.origin`` for what the file actually was.

:func:`build` and :func:`stable_chunk_id` are the producer side: they let
``scripts/ingest_folder.py`` (for local files, which Docling cannot fetch) emit
records that are indistinguishable from the converter's.

Flink-free on purpose, so the producers and the operators cannot drift.
"""

from __future__ import annotations

import hashlib
from typing import Any

# Field names, as the Docling target's defaults spell them. They are
# configurable there (`text_field`, `doc_id_field`, ...); if a deployment
# changes one, change it here too rather than scattering literals.
TEXT = "text"
METADATA = "metadata"
DOC_ID = "doc_id"
CHUNK_INDEX = "chunk_index"
CHUNK_ID = "chunk_id"
HEADINGS = "headings"
PAGE_NUMBERS = "page_numbers"


def doc_id(record: dict[str, Any]) -> str:
    """The document key. All chunks of a document share it, and every keyed
    stage partitions on it."""
    return str(record.get(DOC_ID) or "")


def text(record: dict[str, Any]) -> str:
    return record.get(TEXT) or ""


def chunk_index(record: dict[str, Any]) -> int:
    try:
        return int(record.get(CHUNK_INDEX) or 0)
    except (TypeError, ValueError):
        return 0


def origin(record: dict[str, Any]) -> dict[str, Any]:
    """``metadata.origin`` — mimetype, filename, binary_hash, uri."""
    metadata = record.get(METADATA) or {}
    return metadata.get("origin") or {}


def filename(record: dict[str, Any]) -> str:
    """The document's own name. Often the only human-readable title available:
    ``headings`` are opt-in, and ``doc_id`` may be an opaque key."""
    return str(origin(record).get("filename") or "")


def binary_hash(record: dict[str, Any]) -> str:
    """Hash of the source bytes, as a string.

    A string because it does not fit a signed 64-bit integer — JSON consumers
    and OpenSearch's ``long`` both mangle it. It is the cheapest "is this the
    same file?" test there is, and it is what :func:`stable_chunk_id` keys on.
    """
    value = origin(record).get("binary_hash")
    return "" if value is None else str(value)


def headings(record: dict[str, Any]) -> list[str]:
    """Section path, outermost first — empty when the producer did not ask the
    target for ``headings_field``."""
    value = record.get(HEADINGS)
    if not isinstance(value, (list, tuple)):
        return []
    return [str(h) for h in value if h]


def page_numbers(record: dict[str, Any]) -> list[int]:
    """Sorted page numbers — empty when ``page_field`` was not requested."""
    value = record.get(PAGE_NUMBERS)
    if not isinstance(value, (list, tuple)):
        return []
    pages: set[int] = set()
    for item in value:
        try:
            pages.add(int(item))
        except (TypeError, ValueError):
            continue
    return sorted(pages)


def is_chunk(record: dict[str, Any]) -> bool:
    """True for a chunk, as opposed to anything else that lands on the topic.

    A record without a ``doc_id`` cannot be keyed, and one carrying ``kind`` is
    a control message from some other producer. Either would otherwise be
    enriched, embedded and indexed as an empty document.
    """
    return bool(record.get(DOC_ID)) and "kind" not in record


def document_hash(data: bytes) -> str:
    """Docling's stable hash of a document's bytes — plain ``sha256`` hex.

    Note that this is *not* ``metadata.origin.binary_hash``, which is a
    different, shorter hash and is the one that travels in the record. The file
    hash does not appear in the record at all, which is why a producer that
    wants to reproduce :func:`stable_chunk_id` needs the source bytes.
    """
    return hashlib.sha256(data).hexdigest()


def stable_chunk_id(document_hash_: str, doc_id_: str, chunk_index_: int) -> str:
    """The converter's chunk ID, reproduced exactly.

    ``sha256(f"{document_hash or doc_id}:{doc_id}:{chunk_index}")``, matching
    ``docling_jobkit.connectors.kafka.target_processor._stable_chunk_id``, where
    ``document_hash`` is :func:`document_hash` of the source bytes.

    It has to match rather than merely be deterministic: Kafka is append-only,
    so a re-run after a mid-document failure appends a second copy of the
    chunks, and this ID is what the dedup stage and the index collapse them on.
    The same file converted locally and converted by the service must land on
    the same ID — verified against records the service actually wrote.
    """
    key = f"{document_hash_ or doc_id_}:{doc_id_}:{chunk_index_}"
    return hashlib.sha256(key.encode()).hexdigest()


def build(
    *,
    doc_id: str,
    chunk_index: int,
    text: str,
    document_hash: str = "",
    origin: dict[str, Any] | None = None,
    headings: list[str] | None = None,
    page_numbers: list[int] | None = None,
    has_image: bool = False,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Assemble a record in the wire format, for producers other than Docling.

    Field order follows the converter's, so a diff of two topics lines up.
    Pass ``document_hash`` (see :func:`document_hash`) to get the ID the service
    would have assigned; without it the ID falls back to ``doc_id``-derived,
    which is still deterministic but will not match a service-converted copy.
    """
    record: dict[str, Any] = {
        TEXT: text,
        METADATA: {"origin": dict(origin or {}), "has_image": has_image},
        DOC_ID: doc_id,
        CHUNK_INDEX: chunk_index,
    }
    if page_numbers is not None:
        record[PAGE_NUMBERS] = list(page_numbers)
    if headings is not None:
        record[HEADINGS] = list(headings)
    record[CHUNK_ID] = stable_chunk_id(document_hash, doc_id, chunk_index)
    if extra:
        record.update(extra)
    return record
