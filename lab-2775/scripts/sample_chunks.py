#!/usr/bin/env python3
"""Produce a small, deliberately-crafted document onto the chunk topic.

Exists so a pipeline can be exercised and demoed without a Docling conversion:
the sample contains exactly the things the graph reacts to — PII for the guard,
an exact duplicate for dedup, page furniture and hyphenation for normalize — in
the wire format Docling's ``kafka_chunks`` target writes.

    python scripts/sample_chunks.py --dry-run
    python scripts/sample_chunks.py --topic ws.07.chunks
"""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pipeline.logic import chunk_record  # noqa: E402

DEFAULT_TOPIC = os.environ.get("KAFKA_CHUNKS_TOPIC", "docling.chunks")
DEFAULT_BOOTSTRAP = "localhost:29092"
DOC_ID = "sample/streaming-handbook.pdf"
# Stands in for the hash of the source bytes, so the chunk ids stay stable
# across runs — the same sample produced twice is a duplicate delivery.
DOCUMENT_HASH = "0" * 64

TITLE = "The Streaming Handbook"

# (headings, page, text) — index is the position in the list.
CHUNKS = [
    (
        [TITLE],
        1,
        "This handbook describes how documents are turned into retrievable chunks. "
        "It covers normalization, chunking and indexing in prac-\ntice.",
    ),
    (
        [TITLE, "Contact"],
        2,
        "Questions go to jane.doe@acme.com. Billing uses the card 4111 1111 1111 1111 "
        "and the account DE89 3704 0044 0532 0130 00.\n2\n",
    ),
    (
        [TITLE, "Layout analysis"],
        4,
        "Layout analysis assigns every text block a role: title, paragraph, table or "
        "caption. The model achieves 0.82 mean average precision on the evaluation set, "
        "which is the number most readers care about.",
    ),
    # The same paragraph again, on a later page: an exact duplicate once
    # normalization has run, which is what the dedup stage drops.
    (
        [TITLE, "Layout analysis"],
        5,
        "Layout analysis assigns  every text block a role: title, paragraph, table or "
        "caption. The model achieves 0.82 mean average precision on the evaluation set, "
        "which is the number most readers care about.\n5\n",
    ),
]


def records() -> list[dict]:
    """The crafted document, in the wire format Docling's target writes."""
    return [
        chunk_record.build(
            doc_id=DOC_ID,
            chunk_index=index,
            text=text,
            document_hash=DOCUMENT_HASH,
            origin={
                "mimetype": "application/pdf",
                "binary_hash": None,
                "filename": DOC_ID.rsplit("/", 1)[-1],
                "uri": None,
            },
            headings=headings,
            page_numbers=[page],
        )
        for index, (headings, page, text) in enumerate(CHUNKS)
    ]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--topic", default=DEFAULT_TOPIC)
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    payloads = records()
    if args.dry_run:
        for r in payloads:
            print(json.dumps(r, ensure_ascii=False))
        return

    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": args.bootstrap})
    for record in payloads:
        producer.produce(
            args.topic,
            key=DOC_ID.encode(),
            value=json.dumps(record, ensure_ascii=False).encode(),
        )
        producer.poll(0)
    producer.flush(30)
    print(f"produced {len(payloads)} records -> {args.topic}", file=sys.stderr)


if __name__ == "__main__":
    main()
