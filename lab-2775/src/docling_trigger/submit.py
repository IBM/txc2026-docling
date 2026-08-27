"""The Docling request this app sends — one batch-convert submission per object.

It is the same request ``scripts/saas_ingest.py`` makes, written as a plain
dict instead of through the client SDK. Building the JSON by hand is not
laziness about dependencies (though it does keep the image to "python and a web
server"): the SDK serializes conversion options with ``exclude_defaults=True``,
which drops ``chunking_options.chunker`` — the server's discriminator — and the
request comes back *422 Unable to extract tag using discriminator*. The script
has to monkey-patch the client to put it back. A literal dict cannot lose it.

The request has three parts, from three different places:

* **the source** — the object, resolved from the event and this deployment's
  COS credentials (a presigned URL, or S3 coordinates);
* **the target** — ``kafka_chunks``, the workshop's Kafka cluster from the
  environment, and the *topic from the student's header*. This is the only
  per-student value in the whole payload;
* **the options** — chunker, tokenizer and token budget, from the environment,
  so every student's chunks are sized for the model that will embed them.

``POST {docling_url}/v1/convert/source/batch`` with an ``X-Api-Key`` header
returns a task id immediately and converts in the background, which is exactly
the fire-and-forget shape this app wants: see ``app.py``.
"""

from __future__ import annotations

from typing import Any

from . import presign
from .cosevent import ObjectEvent
from .settings import Env, JobSpec

SUBMIT_PATH = "/v1/convert/source/batch"


def kafka_target(env: Env, topic: str) -> dict[str, Any]:
    """The ``kafka_chunks`` target: where Docling writes one message per chunk.

    ``bootstrap_servers`` is resolved by *Docling*, not by this app — a service
    in someone else's cloud does not see the addresses this container does.
    """
    target: dict[str, Any] = {
        "kind": "kafka_chunks",
        "bootstrap_servers": list(env.bootstrap),
        "topic": topic,
        "verify_certs": env.kafka_verify_certs,
        # All chunks of a document in one partition, in order — what the
        # doc-assembler and reconcile stages downstream key on.
        "key_mode": "doc_id",
    }
    if env.kafka_user and env.kafka_password:
        target["auth"] = {
            "kind": "sasl",
            "mechanism": env.sasl_mechanism,
            "username": env.kafka_user,
            "password": env.kafka_password,
        }
        if env.kafka_ca_cert:
            # The broker's CA, base64 PEM, inside the auth block — this is how
            # a service in someone else's cloud can verify a privately signed
            # listener. The alternative is verify_certs=false, which is what
            # this replaces.
            target["auth"]["ca_cert"] = env.kafka_ca_cert
        target["security_protocol"] = env.security_protocol
    else:
        target["security_protocol"] = "PLAINTEXT"
    if not env.basic:
        # Opt-in on the service. Without them the chunks carry no structure at
        # all, and every downstream stage that cites a page or a section
        # silently degrades.
        target["headings_field"] = "headings"
        target["page_field"] = "page_numbers"
    return target


def convert_options(env: Env) -> dict[str, Any]:
    """Conversion and chunking options — identical for every student."""
    options: dict[str, Any] = {"do_ocr": env.ocr}
    if not env.basic:
        options["chunking_options"] = {
            # The discriminator. Never omit it (see the module docstring).
            "chunker": "hybrid",
            # Chunk boundaries follow the tokenizer of the model that embeds
            # them, so a chunk never overflows the embedding context window.
            "tokenizer": env.tokenizer,
            "max_tokens": env.max_tokens,
            "merge_peers": True,
        }
    return options


def http_source(url: str) -> dict[str, Any]:
    return {"kind": "http", "url": url}


def s3_source(env: Env, event: ObjectEvent) -> dict[str, Any]:
    """S3 coordinates for the object — the alternative to a presigned URL.

    Docling reads the bucket itself, so the HMAC credentials travel with the
    request. Note that ``key_prefix`` is a *prefix*: ``max_num_elements`` caps
    what a key that is also the prefix of another key can drag in.
    """
    return {
        "kind": "s3",
        "endpoint": endpoint_for(env, event),
        "verify_ssl": env.cos_verify_ssl,
        "access_key": env.cos_access_key,
        "secret_key": env.cos_secret_key,
        "bucket": event.bucket,
        "key_prefix": event.key,
        "max_num_elements": 1,
    }


def endpoint_for(env: Env, event: ObjectEvent) -> str:
    """The S3 endpoint to read the object from.

    The event may name one (the captured sample leaves it empty), so the
    deployment's ``COS_ENDPOINT`` — or the region it is derived from — is what
    normally answers this.
    """
    return event.endpoint or env.cos_endpoint or presign.endpoint_for(env.cos_region)


def source_for(env: Env, event: ObjectEvent) -> dict[str, Any]:
    """One source descriptor for the object, in the configured mode."""
    if env.source_mode == "s3":
        return s3_source(env, event)
    return http_source(
        presign.presigned_get(
            endpoint=endpoint_for(env, event),
            bucket=event.bucket,
            key=event.key,
            access_key=env.cos_access_key,
            secret_key=env.cos_secret_key,
            region=env.cos_region,
            expires=env.presign_expires_s,
        )
    )


def build_request(env: Env, spec: JobSpec, event: ObjectEvent) -> dict[str, Any]:
    """The complete ``/v1/convert/source/batch`` body for one uploaded object."""
    return {
        "options": convert_options(env),
        "sources": [source_for(env, event)],
        "target": kafka_target(env, spec.topic),
    }


def submit_url(docling_url: str) -> str:
    return f"{docling_url.rstrip('/')}{SUBMIT_PATH}"


def auth_headers(spec: JobSpec) -> dict[str, str]:
    return {"X-Api-Key": spec.api_key} if spec.api_key else {}


def redact(payload: dict[str, Any]) -> dict[str, Any]:
    """A copy safe to log: no Kafka password, no COS secret, no signature."""
    import copy

    safe = copy.deepcopy(payload)
    auth = safe.get("target", {}).get("auth")
    if isinstance(auth, dict):
        if "password" in auth:
            auth["password"] = "***"
        if auth.get("ca_cert"):
            # Not secret, but 2 KB of base64 makes a log line unreadable.
            auth["ca_cert"] = f"<{len(auth['ca_cert'])} bytes of base64 PEM>"
    for source in safe.get("sources", []):
        if source.get("kind") == "s3":
            source["secret_key"] = "***"
        elif source.get("kind") == "http":
            source["url"] = source.get("url", "").split("?", 1)[0] + "?<signed>"
    return safe
