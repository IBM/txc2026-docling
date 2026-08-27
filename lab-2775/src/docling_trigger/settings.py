"""What the app is configured with, and what each student's event carries.

The split is the whole design of this app, so it is worth stating plainly:

* **Environment** is what is true for the *class* — the Kafka cluster, the COS
  credentials, the chunker settings. It is baked into the one deployment.
* **Headers** are what is true for one *student* — their Docling instance, its
  API key, and their chunk topic. They ride on the event notification, which is
  the only per-student thing a student can configure without redeploying
  anything.

So the app is deployed once and holds no student state at all: two events from
two students differ only in three header values, and nothing in between them is
cached, keyed or reserved per student.

Every header has an environment default, which is what makes a single-tenant
smoke test possible (``curl`` the app with no headers at all and it uses the
deployment's own Docling instance and topic). Only the topic has no sane shared
default in a workshop, so a missing ``X-Kafka-Topic`` with no
``KAFKA_CHUNKS_TOPIC`` set is the one configuration error that is answered with
a 400 rather than guessed at.
"""

from __future__ import annotations

import base64
import binascii
import os
from dataclasses import dataclass, field
from typing import Mapping

# --- the three things a student sets on their bucket's subscription --------
#
# Two spellings of each, because two mechanisms can carry them and which one a
# student has depends on how the subscription was made:
#
#   * a CloudEvent **extension attribute** — what
#     ``ibmcloud ce sub cos create --extension doclingurl=...`` sets. The
#     CloudEvents spec restricts these names to lowercase alphanumerics (no
#     dashes, no dots), and the binary HTTP binding delivers them prefixed:
#     ``doclingurl`` arrives as the header ``Ce-Doclingurl``, and appears as a
#     top-level attribute if the event is delivered structured;
#   * a plain **HTTP header**, for anything that can set one directly.
#
# So each value is read from ``X-Docling-Url`` *or* ``Ce-Doclingurl`` *or* the
# CloudEvent attribute ``doclingurl``, and a student never has to know which of
# the three they are actually using.
DOCLING_URL_KEYS = ("x-docling-url", "ce-doclingurl", "doclingurl")
DOCLING_KEY_KEYS = ("x-docling-api-key", "ce-doclingkey", "doclingkey")
TOPIC_KEYS = ("x-kafka-topic", "ce-chunkstopic", "chunkstopic")
STUDENT_KEYS = ("x-student-id", "ce-studentid", "studentid")

# What to tell a student to set. The header spelling and the extension
# spelling of the same three values.
H_DOCLING_URL = "X-Docling-Url"
H_DOCLING_KEY = "X-Docling-Api-Key"
H_TOPIC = "X-Kafka-Topic"
H_STUDENT = "X-Student-Id"
E_DOCLING_URL = "doclingurl"
E_DOCLING_KEY = "doclingkey"
E_TOPIC = "chunkstopic"
E_STUDENT = "studentid"

# Formats Docling converts. A bucket collects more than documents — an editor's
# swap file, a .DS_Store, the notes someone dropped next to the PDF — and every
# one of those would otherwise become a failed conversion the student has to
# explain. Empty TRIGGER_SUFFIXES disables the filter.
DEFAULT_SUFFIXES = (
    ".pdf,.docx,.pptx,.xlsx,.md,.markdown,.html,.htm,.xhtml,.adoc,.asciidoc,"
    ".csv,.txt,.xml,.png,.jpg,.jpeg,.tif,.tiff,.bmp,.webp"
)


def _flag(name: str, default: bool) -> bool:
    """A boolean from the environment. Empty means *unset*, not False.

    That matters because ``deploy.sh`` passes every variable through whether it
    is set or not, and an empty ``KAFKA_VERIFY_CERTS`` must not override the
    default that a configured CA certificate implies.
    """
    raw = os.environ.get(name, "").strip()
    return default if not raw else raw.lower() in ("1", "true", "yes", "on")


def ca_cert_b64(raw: str) -> str:
    """The CA certificate as the ``kafka_chunks`` target wants it: base64 PEM.

    Accepts either spelling, because both are natural depending on where it
    comes from: a PEM read straight off disk (``KAFKA_CA_LOCATION``, what Flink
    and the laptop scripts use) or an already-base64 blob (``KAFKA_CA_CERT_B64``,
    what survives being an environment variable and a Code Engine secret).

    Validated rather than passed through: a wrong CA does not fail here, it
    fails much later inside Docling as a TLS handshake this app never sees.
    """
    text = (raw or "").strip()
    if not text:
        return ""
    if text.startswith("-----BEGIN"):
        # Normalised to exactly one trailing newline: a PEM without it is
        # rejected by some parsers, and two of them is not the same base64.
        return base64.b64encode((text + "\n").encode("utf-8")).decode("ascii")
    try:
        decoded = base64.b64decode(text, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError(f"the CA certificate is neither PEM nor valid base64: {exc}") from None
    if b"BEGIN CERTIFICATE" not in decoded:
        raise ValueError("the base64 CA certificate does not decode to a PEM certificate")
    return text


def _first(*names_and_default: str) -> str:
    """First non-empty environment variable among ``names``; last arg is the default."""
    *names, default = names_and_default
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return default


def _load_ca() -> tuple[str, str]:
    """``(base64 PEM, error)`` from the environment — never raising.

    A misconfigured CA must not stop the app from starting: it starts, says so
    on ``/`` and in ``/health``, and submits without one, which is a failure
    somebody can read rather than a container that will not boot.
    """
    raw = _first("KAFKA_CA_CERT_B64", "KAFKA_CA_CERT", "")
    if not raw:
        path = _first("KAFKA_CA_LOCATION", "")
        if path:
            try:
                raw = open(path, encoding="utf-8").read()
            except OSError as exc:
                return "", f"KAFKA_CA_LOCATION={path!r} cannot be read: {exc}"
    try:
        return ca_cert_b64(raw), ""
    except ValueError as exc:
        return "", str(exc)


@dataclass(frozen=True)
class Env:
    """Process-wide configuration, read once at startup."""

    # --- Kafka, as *Docling* reaches it (not as this app or a laptop does) --
    bootstrap: list[str] = field(default_factory=list)
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "PLAIN"
    kafka_user: str = ""
    kafka_password: str = ""
    kafka_verify_certs: bool = False
    # The broker's CA, base64 PEM, handed to Docling inside the target's auth
    # block. The lab brokers are signed by a private CA, so without it the only
    # way the hosted service can produce to them is with verification off.
    kafka_ca_cert: str = ""
    kafka_ca_error: str = ""
    default_topic: str = ""
    topic_template: str = ""

    # --- COS: how the object is handed to Docling ---------------------------
    source_mode: str = "presigned"  # presigned | s3
    cos_endpoint: str = ""
    cos_region: str = ""
    cos_access_key: str = ""
    cos_secret_key: str = ""
    cos_verify_ssl: bool = True
    presign_expires_s: int = 3600

    # --- Docling ------------------------------------------------------------
    docling_url: str = ""
    docling_api_key: str = ""
    submit_timeout_s: float = 30.0
    tokenizer: str = "ibm-granite/granite-embedding-278m-multilingual"
    max_tokens: int = 512
    ocr: bool = False
    basic: bool = False

    # --- this app -----------------------------------------------------------
    suffixes: tuple[str, ...] = ()
    max_concurrency: int = 16
    dedup_ttl_s: float = 300.0

    @classmethod
    def from_environ(cls) -> "Env":
        raw_bootstrap = _first("DOCLING_KAFKA_BOOTSTRAP", "KAFKA_BOOTSTRAP_SERVERS", "")
        suffixes = os.environ.get("TRIGGER_SUFFIXES", DEFAULT_SUFFIXES)
        ca_cert, ca_error = _load_ca()
        return cls(
            bootstrap=[s.strip() for s in raw_bootstrap.split(",") if s.strip()],
            security_protocol=_first("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
            sasl_mechanism=_first("KAFKA_SASL_MECHANISM", "PLAIN"),
            kafka_user=_first("KAFKA_API_KEY", ""),
            kafka_password=_first("KAFKA_API_SECRET", ""),
            # Off unless a CA is configured — the lab brokers are signed by a
            # private CA that Docling SaaS has no reason to trust. Supplying
            # one is what makes verification possible, so it also turns it on;
            # KAFKA_VERIFY_CERTS still overrides either way.
            kafka_verify_certs=_flag("KAFKA_VERIFY_CERTS", bool(ca_cert)),
            kafka_ca_cert=ca_cert,
            kafka_ca_error=ca_error,
            default_topic=_first("KAFKA_CHUNKS_TOPIC", ""),
            topic_template=_first("TRIGGER_TOPIC_TEMPLATE", ""),
            source_mode=_first("COS_SOURCE_MODE", "presigned").lower(),
            cos_endpoint=_first("COS_ENDPOINT", "").replace("https://", "").replace("http://", "").rstrip("/"),
            cos_region=_first("COS_BUCKET_REGION", "COS_REGION", ""),
            cos_access_key=_first("COS_ACCESS_KEY_ID", "COS_HMAC_ACCESS_KEY_ID", ""),
            cos_secret_key=_first("COS_SECRET_ACCESS_KEY", "COS_HMAC_SECRET_ACCESS_KEY", ""),
            cos_verify_ssl=_flag("COS_VERIFY_SSL", True),
            presign_expires_s=int(_first("PRESIGN_EXPIRES_S", "3600")),
            docling_url=_first("DOCLING_SERVICE_URL", "DOCLING_BASE_URL", "").rstrip("/"),
            docling_api_key=_first("DOCLING_SERVICE_API_KEY", "DOCLING_API_KEY", ""),
            submit_timeout_s=float(_first("DOCLING_SUBMIT_TIMEOUT_S", "30")),
            tokenizer=_first("CHUNK_TOKENIZER_ID", "ibm-granite/granite-embedding-278m-multilingual"),
            max_tokens=int(_first("EMBEDDING_MAX_TOKENS", "512")),
            ocr=_flag("DOCLING_OCR", False),
            basic=_flag("DOCLING_BASIC", False),
            suffixes=tuple(s.strip().lower() for s in suffixes.split(",") if s.strip()),
            max_concurrency=int(_first("TRIGGER_MAX_CONCURRENCY", "16")),
            dedup_ttl_s=float(_first("TRIGGER_DEDUP_TTL_S", "300")),
        )

    def problems(self) -> list[str]:
        """Configuration that is missing or contradictory, for the readiness probe.

        Reported rather than raised: an app that refuses to start is an app whose
        error message nobody reads. It starts, answers ``/health``, and says
        exactly what is wrong on ``/`` and in the 500 body.
        """
        bad: list[str] = []
        if not self.bootstrap:
            bad.append("KAFKA_BOOTSTRAP_SERVERS is empty — Docling has nowhere to write the chunks")
        if self.security_protocol.startswith("SASL") and not (self.kafka_user and self.kafka_password):
            bad.append("KAFKA_SECURITY_PROTOCOL is SASL but KAFKA_API_KEY / KAFKA_API_SECRET are unset")
        if self.kafka_ca_error:
            bad.append(self.kafka_ca_error)
        if self.kafka_verify_certs and not self.kafka_ca_cert:
            bad.append(
                "KAFKA_VERIFY_CERTS is on but no CA is configured — Docling will reject a "
                "privately signed broker certificate (set KAFKA_CA_CERT_B64 or KAFKA_CA_LOCATION)"
            )
        if self.source_mode not in ("presigned", "s3"):
            bad.append(f"COS_SOURCE_MODE={self.source_mode!r} is neither 'presigned' nor 's3'")
        if not (self.cos_access_key and self.cos_secret_key):
            bad.append("COS_ACCESS_KEY_ID / COS_SECRET_ACCESS_KEY are unset — the object cannot be read")
        if not (self.cos_endpoint or self.cos_region):
            bad.append("neither COS_ENDPOINT nor COS_BUCKET_REGION is set — no S3 endpoint to read from")
        return bad


@dataclass(frozen=True)
class JobSpec:
    """The three per-student values, resolved from one request's headers."""

    docling_url: str
    api_key: str
    topic: str
    student: str

    @classmethod
    def from_headers(
        cls,
        headers: Mapping[str, str],
        env: Env,
        attributes: Mapping[str, object] | None = None,
    ) -> "JobSpec":
        """Resolve one delivery's headers (and CloudEvent attributes) against the defaults.

        ``attributes`` is the body of a structured CloudEvent, where extensions
        are top-level fields rather than ``Ce-*`` headers. Lookup is
        case-insensitive throughout, since the casing of a header is not
        something the sender guarantees.
        """
        found = {str(k).lower(): str(v).strip() for k, v in headers.items()}
        for key, value in (attributes or {}).items():
            found.setdefault(str(key).lower(), str(value).strip() if value is not None else "")

        def pick(names: tuple[str, ...]) -> str:
            return next((found[n] for n in names if found.get(n)), "")

        student = pick(STUDENT_KEYS)
        topic = pick(TOPIC_KEYS)
        if not topic and student and env.topic_template:
            topic = env.topic_template.format(id=student, student=student)
        return cls(
            docling_url=(pick(DOCLING_URL_KEYS) or env.docling_url).rstrip("/"),
            api_key=pick(DOCLING_KEY_KEYS) or env.docling_api_key,
            topic=topic or env.default_topic,
            student=student,
        )

    def problems(self) -> list[str]:
        """Per-request configuration errors, phrased as the header to fix."""
        bad = []
        if not self.docling_url:
            bad.append(f"no Docling service URL: set {H_DOCLING_URL} (or the {E_DOCLING_URL} extension)")
        if not self.topic:
            bad.append(f"no chunk topic: set {H_TOPIC} (or the {E_TOPIC} extension)")
        return bad

    def redacted(self) -> dict:
        return {
            "docling_url": self.docling_url,
            "api_key": "set" if self.api_key else "",
            "topic": self.topic,
            "student": self.student,
        }
