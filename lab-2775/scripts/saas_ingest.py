#!/usr/bin/env python3
"""Submit documents to Docling and have it stream the chunks onto Kafka.

This is the whole producer side of the pipeline. Conversion *and* chunking run
inside Docling; its ``kafka_chunks`` target writes one Kafka message per chunk,
so nothing is downloaded, converted or chunked on this machine and Flink starts
from a topic that is already full of chunks.

Two deployments, one script — they differ only in URL, key and broker:

    # local: docling-serve on the laptop -> the compose broker
    python scripts/saas_ingest.py --docling-url http://localhost:5001 \
        --bootstrap localhost:29092 https://arxiv.org/pdf/2206.01062

    # hosted: Docling SaaS -> the Confluent brokers
    python scripts/saas_ingest.py --load-env

Sources are HTTP(S) URLs the service can reach. Local files are **not** usable
here, and that is a property of the service rather than of this script: the
``kafka_chunks`` target exists only on the source endpoints (the file-upload
endpoint answers inbody/presigned_url/zip), and docling-core refuses any source
URL that is not globally routable, so serving a file from this machine is
rejected too. To put local files on the same topic, convert them on the laptop:

    python scripts/ingest_folder.py ./docs --topic docling.chunks   # make ingest-files

which emits the same record shape. With no sources given, the built-in sample
documents are used.

By default the target is asked for ``headings`` and ``page_numbers`` and the
chunker is tokenized against the embedding model, so the chunks carry the
structure the Flink stages use and are sized for the model that will embed
them. Both are opt-in on the service; ``--basic`` turns them off for an older
deployment that does not accept the options.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys

# ---------------------------------------------------------------------------
# Early .env loading — must happen before os.environ is read for defaults.
# Pass --load-env (or set LOAD_DOTENV=1) to opt in.
# ---------------------------------------------------------------------------
if "--load-env" in sys.argv or os.environ.get("LOAD_DOTENV") == "1":
    try:
        from dotenv import load_dotenv

        _env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
        load_dotenv(dotenv_path=_env_path, override=False)
    except ImportError:
        sys.exit("--load-env requires python-dotenv: pip install python-dotenv")

sys.path.insert(0, os.path.dirname(__file__))

from labtools.kafka import client_config, ensure_topic

DEFAULT_DOCLING_URL = "http://localhost:5001"
DEFAULT_TOPIC = os.environ.get("KAFKA_CHUNKS_TOPIC", "docling.chunks")
# The tokenizer that sizes chunks — a HuggingFace repo id, not the
# watsonx.ai model id that EMBEDDING_MODEL_ID now holds.
DEFAULT_MODEL = os.environ.get("CHUNK_TOKENIZER_ID", "ibm-granite/granite-embedding-278m-multilingual")
DEFAULT_MAX_TOKENS = int(os.environ.get("EMBEDDING_MAX_TOKENS", "512"))

SAMPLE_URLS = [
    "https://arxiv.org/pdf/2206.01062",  # DocLayNet
    "https://arxiv.org/pdf/2501.17887",  # Docling technical report
    "https://arxiv.org/pdf/2311.18481",  # ESG DocQA
]


def split_sources(items: list[str]) -> list[str]:
    """Keep the URLs; refuse local paths, with the working alternative.

    Rejected here rather than at the service, where a local path comes back as
    an opaque ``num_failed=1`` with no per-document reason.
    """
    urls, local = [], []
    for item in items:
        (urls if item.startswith(("http://", "https://")) else local).append(item)
    if local:
        joined = " ".join(local)
        sys.exit(
            f"local paths are not supported by the kafka_chunks target: {joined}\n"
            "Docling accepts only globally routable source URLs, and its file-upload\n"
            "endpoint cannot write to Kafka. Convert them on this machine instead:\n"
            f"  python scripts/ingest_folder.py {joined} --topic <topic>"
        )
    return urls


def _client(url: str, api_key: str, timeout: float):
    """The Docling client, with the chunker discriminator put back.

    ``DoclingServiceClient`` serializes conversion options with
    ``exclude_defaults=True``, which drops ``chunking_options.chunker`` whenever
    it equals the model default — and that field is the discriminator the
    server uses to pick a chunker, so the request comes back 422
    "Unable to extract tag using discriminator 'chunker'". Re-adding it after
    serialization is the smallest fix that does not give up on configuring the
    chunker; drop this override once the client stops excluding it.
    """
    from docling.service_client import DoclingServiceClient

    class _Client(DoclingServiceClient):
        def _serialize_convert_options(self, options):
            payload = super()._serialize_convert_options(options)
            chunking = getattr(options, "chunking_options", None)
            chunker = getattr(chunking, "chunker", None)
            if chunker and isinstance(payload.get("chunking_options"), dict):
                payload["chunking_options"].setdefault("chunker", chunker)
            return payload

    return _Client(url=url, api_key=api_key, job_timeout=timeout)


def read_ca_cert(path: str) -> str:
    """The PEM at ``path``, base64-encoded — the shape the target expects.

    The same file Flink's Java client gets as a truststore (KAFKA_CA_LOCATION);
    Docling is somewhere else entirely, so it gets the bytes instead of a path.
    """
    if not path:
        return ""
    try:
        pem = open(path, "rb").read()
    except OSError as exc:
        sys.exit(f"--ca-cert {path}: {exc}")
    if b"BEGIN CERTIFICATE" not in pem:
        sys.exit(f"--ca-cert {path}: not a PEM certificate")
    return base64.b64encode(pem).decode("ascii")


def build_target(args) -> dict:
    """The ``kafka_chunks`` target descriptor.

    Note that ``bootstrap_servers`` is resolved by *Docling*, not by this
    script: a service running in a container or in the cloud does not see the
    same addresses this laptop does.
    """
    target: dict = {
        "kind": "kafka_chunks",
        "bootstrap_servers": [s.strip() for s in args.bootstrap.split(",") if s.strip()],
        "topic": args.topic,
        "verify_certs": args.verify_certs,
        # All chunks of a document in one partition, in order — which is what
        # the doc-assembler and reconcile stages key on.
        "key_mode": "doc_id",
    }
    if args.kafka_user and args.kafka_password:
        target["auth"] = {
            "kind": "sasl",
            "mechanism": os.environ.get("KAFKA_SASL_MECHANISM", "PLAIN"),
            "username": args.kafka_user,
            "password": args.kafka_password,
        }
        ca_cert = read_ca_cert(args.ca_cert)
        if ca_cert:
            # The broker's CA, base64 PEM, inside the auth block: it is what
            # lets Docling *verify* a privately signed listener instead of
            # being told to skip verification. Same field the Code Engine
            # trigger sends (trigger/docling_trigger/submit.py).
            target["auth"]["ca_cert"] = ca_cert
        target["security_protocol"] = os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    else:
        target["security_protocol"] = "PLAINTEXT"

    if not args.basic:
        # Off by default on the service: without these the chunks carry no
        # structure at all, and every downstream stage that cites a page or a
        # section silently degrades.
        target["headings_field"] = "headings"
        target["page_field"] = "page_numbers"
    return target


def build_options(args):
    from docling.datamodel.service.options import ConvertDocumentsOptions

    kwargs: dict = {"do_ocr": args.ocr}
    if not args.basic:
        kwargs["chunking_options"] = {
            "chunker": "hybrid",
            # Chunk boundaries follow the tokenizer of the model that embeds
            # them, so a chunk never overflows the embedding context window.
            "tokenizer": args.model,
            "max_tokens": args.max_tokens,
            "merge_peers": True,
        }
    return ConvertDocumentsOptions(**kwargs)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("sources", nargs="*", help="document URLs (default: the built-in samples)")
    ap.add_argument(
        "--docling-url",
        default=os.environ.get("DOCLING_SERVICE_URL") or os.environ.get("DOCLING_BASE_URL") or DEFAULT_DOCLING_URL,
        help=f"Docling service base URL (default: env, else {DEFAULT_DOCLING_URL})",
    )
    ap.add_argument(
        "--api-key",
        default=os.environ.get("DOCLING_SERVICE_API_KEY") or os.environ.get("DOCLING_API_KEY", ""),
        help="Docling API key; empty for a local docling-serve without auth",
    )
    ap.add_argument(
        "--bootstrap",
        default=os.environ.get("DOCLING_KAFKA_BOOTSTRAP") or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", ""),
        help="brokers *as Docling reaches them* (default: DOCLING_KAFKA_BOOTSTRAP, else KAFKA_BOOTSTRAP_SERVERS)",
    )
    ap.add_argument("--topic", default=DEFAULT_TOPIC, help=f"target topic (default: {DEFAULT_TOPIC})")
    ap.add_argument("--kafka-user", default=os.environ.get("KAFKA_API_KEY", ""))
    ap.add_argument("--kafka-password", default=os.environ.get("KAFKA_API_SECRET", ""))
    ap.add_argument(
        "--ca-cert",
        default=os.environ.get("KAFKA_CA_LOCATION", ""),
        help="PEM of the CA that signed the brokers; sent base64 in the target's auth "
        "(default: KAFKA_CA_LOCATION). Implies --verify-certs.",
    )
    ap.add_argument(
        "--verify-certs",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="verify the broker TLS certificate from Docling "
        "(default: yes when --ca-cert is given, no otherwise — the lab CA is private)",
    )
    ap.add_argument("--model", default=DEFAULT_MODEL, help="tokenizer driving chunk sizes")
    ap.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS)
    ap.add_argument("--ocr", action=argparse.BooleanOptionalAction, default=False, help="run OCR (default: no)")
    ap.add_argument(
        "--basic",
        action="store_true",
        help="omit the chunker and structure options, for a service that does not accept them",
    )
    ap.add_argument("--timeout", type=float, default=float(os.environ.get("DOCLING_TIMEOUT_S", "900")))
    ap.add_argument(
        "--no-create-topic",
        dest="create_topic",
        action="store_false",
        help="do not create the topic first (the brokers here do not auto-create)",
    )
    ap.add_argument("--sample", action="store_true", help="use the built-in sample documents")
    ap.add_argument("--dry-run", action="store_true", help="print the request that would be sent and exit")
    ap.add_argument(
        "--load-env",
        action="store_true",
        default=os.environ.get("LOAD_DOTENV") == "1",
        help="load .env from the repo root before resolving defaults (requires python-dotenv)",
    )
    args = ap.parse_args()

    # Verification is only possible with a CA to verify against, so the CA is
    # what decides it unless the flag was given explicitly.
    if args.verify_certs is None:
        args.verify_certs = bool(args.ca_cert)

    if not args.bootstrap:
        sys.exit("no Kafka brokers: pass --bootstrap, or set kafka.bootstrap_servers in lab.yaml")
    items = args.sources or SAMPLE_URLS
    if args.sample:
        items = SAMPLE_URLS
    # Validated before anything is created or submitted.
    urls = split_sources(items)

    target = build_target(args)
    if args.dry_run:
        redacted = json.loads(json.dumps(target))
        if "auth" in redacted:
            redacted["auth"]["password"] = "***"
            if redacted["auth"].get("ca_cert"):
                redacted["auth"]["ca_cert"] = f"<{len(redacted['auth']['ca_cert'])} bytes of base64 PEM>"
        print(json.dumps({"docling_url": args.docling_url, "sources": urls, "target": redacted}, indent=2))
        return

    if args.create_topic:
        # Docling's producer would fail on a missing topic: auto-creation is off
        # on these brokers, exactly as it is on Confluent Cloud.
        ensure_topic(client_config(), args.topic)

    from docling.datamodel.service.requests import AnyHttpSourceRequest
    from docling.service_client import GenericTargetRequest
    from docling.service_client.exceptions import TaskExecutionError

    print(
        f"submitting {len(urls)} URL(s) to {args.docling_url}\n"
        f"  -> kafka {args.bootstrap} topic {args.topic!r} "
        f"({'SASL' if 'auth' in target else 'PLAINTEXT'})",
        file=sys.stderr,
    )

    with _client(args.docling_url, args.api_key, args.timeout) as client:
        job = client.submit_batch(
            sources=[AnyHttpSourceRequest(url=u) for u in urls],
            target=GenericTargetRequest(**target),
            options=build_options(args),
        )
        try:
            result = job.result(timeout=args.timeout)
        except TaskExecutionError as exc:
            _report_failure(exc.failure, f"conversion failed: {exc}")
            sys.exit(1)

    print(
        f"{result.num_succeeded} succeeded / {result.num_failed} failed / "
        f"{result.num_partially_succeeded} partial  ({result.processing_time:.1f}s)"
    )
    if result.num_failed:
        last = getattr(job, "_last_status", None)
        if last is not None:
            _report_failure(last.failure, last.error_message)

    print(f"\nread the chunks back with:\n  python scripts/drain_topic.py --topic {args.topic}", file=sys.stderr)
    if result.num_failed:
        sys.exit(1)


def _report_failure(failure, message: str | None) -> None:
    if message:
        print(f"Error: {message}", file=sys.stderr)
    if failure is None:
        return
    print(f"  category : {failure.category}", file=sys.stderr)
    print(f"  phase    : {failure.phase}", file=sys.stderr)
    print(f"  retryable: {failure.retryable}", file=sys.stderr)
    for key, value in (failure.details or {}).items():
        print(f"  {key}: {value}", file=sys.stderr)


if __name__ == "__main__":
    main()
