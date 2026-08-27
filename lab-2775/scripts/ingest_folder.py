#!/usr/bin/env python3
"""Convert local documents and produce one Kafka message per chunk.

The companion to ``scripts/saas_ingest.py``, and the reason both exist: the
Docling ``kafka_chunks`` target only accepts globally routable source URLs, so
a file on this laptop can never reach it. This script closes that gap — it
converts and chunks here and produces to the same topic, in the record layout
of :mod:`producer.chunking`, which the jobs read unchanged (the adapter stage
recognises it and passes it straight through).

Use ``saas_ingest.py`` for URLs — it is the real pipeline, with no conversion
on this machine. Use this one for local files.

Conversion goes through a docling-serve / SaaS endpoint when one is configured
(``DOCLING_SERVICE_URL`` / ``DOCLING_BASE_URL``, with an API key if the service
needs one) and falls back to the local ``docling`` package otherwise. Pass
``--via serve`` or ``--via local`` to pin one instead of deciding by key.

    pip install confluent-kafka docling            # 'docling' only for --via local

    python scripts/ingest_folder.py ./docs
    python scripts/ingest_folder.py report.pdf notes.docx --topic docling.chunks
    python scripts/ingest_folder.py ./docs --via local --dry-run

Message layout is the converter's, exactly: key = ``doc_id``, headers
``doc_id`` / ``chunk_index`` / ``chunk_id``, value = the chunk record from
:mod:`pipeline.logic.chunk_record`. The chunk ids match too — they are derived from
the file's own hash with the same scheme the service uses, so converting a file
here and converting it there produce the same ids and the pipeline's dedup
collapses them.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from labtools.kafka import client_config, ensure_topic, require_confluent_kafka

from pipeline.logic import chunk_record  # noqa: E402
from producer.chunking import build_chunker, chunk_records  # noqa: E402

DEFAULT_EXTS = "pdf,docx,pptx,xlsx,html,htm,md,adoc,csv,png,jpg,jpeg,tiff"
DEFAULT_TOPIC = os.environ.get("KAFKA_CHUNKS_TOPIC", "docling.chunks")
# The tokenizer that sizes chunks — a HuggingFace repo id, not the
# watsonx.ai model id that EMBEDDING_MODEL_ID now holds.
DEFAULT_MODEL = os.environ.get("CHUNK_TOKENIZER_ID", "ibm-granite/granite-embedding-278m-multilingual")
DEFAULT_MAX_TOKENS = int(os.environ.get("EMBEDDING_MAX_TOKENS", "512"))


def find_files(paths: list[Path], exts: set[str], recursive: bool) -> list[tuple[Path, str]]:
    """Expand the arguments into ``(path, doc_id)`` pairs.

    A folder contributes every matching file under it, identified by its path
    relative to that folder; a file contributes itself, identified by its
    filename. That identifier becomes the record's ``doc_id`` — the Kafka
    message key, and what every keyed stage groups a document by.
    """
    found: list[tuple[Path, str]] = []
    for item in paths:
        if item.is_dir():
            walk = item.rglob("*") if recursive else item.glob("*")
            for f in sorted(walk):
                if f.is_file() and f.suffix.lower().lstrip(".") in exts:
                    found.append((f, str(f.relative_to(item))))
        elif item.is_file():
            found.append((item, item.name))
        else:
            sys.exit(f"no such file or directory: {item}")
    return found


def make_converter(args):
    """Return ``path -> DoclingDocument`` for the selected conversion backend.

    ``--via auto`` (the default) prefers the SaaS endpoint and only converts
    locally when there is no API key to use it with.
    """
    api_key = os.environ.get("DOCLING_SERVICE_API_KEY") or os.environ.get("DOCLING_API_KEY") or ""
    via = args.via
    if via == "auto":
        # A local docling-serve needs no key, so the presence of a *URL* is the
        # signal, not the presence of a key.
        via = "serve" if args.docling_url else "local"
        if via == "local":
            print(
                "no Docling endpoint configured — converting locally "
                "(set DOCLING_SERVICE_URL, or pass --via local to silence this)",
                file=sys.stderr,
            )

    if via == "local":
        from docling.document_converter import DocumentConverter

        converter = DocumentConverter()
        return lambda path: converter.convert(str(path)).document

    from docling_core.types.doc.document import DoclingDocument

    from producer.docling_client import DoclingClient

    if not args.docling_url:
        sys.exit("--via serve needs a Docling endpoint (DOCLING_SERVICE_URL, or --docling-url)")
    print(f"converting via Docling at {args.docling_url}", file=sys.stderr)
    client = DoclingClient(
        base_url=args.docling_url,
        api_key=api_key,
        timeout_s=float(os.environ.get("DOCLING_TIMEOUT_S", "900")),
    )
    return lambda path: DoclingDocument.model_validate(client.convert_file(str(path)))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("paths", type=Path, nargs="+", help="documents and/or folders to convert")
    ap.add_argument("--topic", default=DEFAULT_TOPIC, help=f"target topic (default: {DEFAULT_TOPIC})")
    ap.add_argument("--bootstrap", help="Kafka bootstrap servers (default: env or localhost:29092)")
    ap.add_argument(
        "--via",
        choices=("auto", "serve", "local"),
        default="auto",
        help="conversion backend: auto = Docling SaaS if DOCLING_API_KEY is set, else local (default)",
    )
    ap.add_argument(
        "--docling-url",
        default=os.environ.get("DOCLING_SERVICE_URL") or os.environ.get("DOCLING_BASE_URL", ""),
        help="docling-serve / SaaS base URL (default: DOCLING_SERVICE_URL, else DOCLING_BASE_URL)",
    )
    ap.add_argument("--ext", default=DEFAULT_EXTS, help=f"comma-separated extensions (default: {DEFAULT_EXTS})")
    ap.add_argument("--no-recursive", dest="recursive", action="store_false", help="do not descend into subfolders")
    ap.add_argument("--limit", type=int, help="stop after N files")
    ap.add_argument("--model", default=DEFAULT_MODEL, help="tokenizer/embedding model driving chunk sizes")
    ap.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS)
    ap.add_argument("--partitions", type=int, default=3, help="partitions if the topic must be created")
    ap.add_argument("--dry-run", action="store_true", help="print the chunk records instead of producing them")
    args = ap.parse_args()

    files = find_files(args.paths, {e.strip().lower() for e in args.ext.split(",") if e.strip()}, args.recursive)
    if args.limit:
        files = files[: args.limit]
    if not files:
        sys.exit(f"no matching documents in {' '.join(str(p) for p in args.paths)} (extensions: {args.ext})")

    producer = None
    if not args.dry_run:
        require_confluent_kafka()
        from confluent_kafka import Producer

        conf = client_config(args.bootstrap)
        ensure_topic(conf, args.topic, partitions=args.partitions)
        producer = Producer(conf)

    print(f"chunking with {args.model} (max_tokens={args.max_tokens})", file=sys.stderr)
    chunker = build_chunker(args.model, args.max_tokens)
    convert = make_converter(args)

    total = 0
    for n, (path, doc_id) in enumerate(files, 1):
        started = time.monotonic()
        # The service derives the chunk id from the hash of the source bytes;
        # this is the one thing that cannot be recovered from the converted
        # document, so it is computed here before conversion.
        document_hash = chunk_record.document_hash(path.read_bytes())
        try:
            doc = convert(path)
        except Exception as exc:  # noqa: BLE001 — one bad file must not stop the batch
            print(f"[{n}/{len(files)}] {doc_id}: conversion FAILED: {exc}", file=sys.stderr)
            continue

        count = 0
        for record in chunk_records(chunker, doc, doc_id, document_hash=document_hash):
            payload = json.dumps(record, ensure_ascii=False)
            if args.dry_run:
                print(payload)
            else:
                # key_mode="doc_id", as the target's default: all chunks of a
                # document in one partition, in order. Headers duplicate the
                # ids so a consumer can route without deserializing the value.
                producer.produce(
                    args.topic,
                    key=doc_id.encode(),
                    value=payload.encode(),
                    headers=[
                        ("doc_id", doc_id),
                        ("chunk_index", str(record["chunk_index"])),
                        ("chunk_id", record["chunk_id"]),
                    ],
                )
                producer.poll(0)
            count += 1

        total += count
        print(
            f"[{n}/{len(files)}] {doc_id}: {count} chunks in {time.monotonic() - started:.1f}s",
            file=sys.stderr,
        )

    if producer is not None:
        producer.flush(30)
        print(f"produced {total} chunk messages -> {args.topic}", file=sys.stderr)
    else:
        print(f"{total} chunk records (dry run, nothing produced)", file=sys.stderr)


if __name__ == "__main__":
    main()
