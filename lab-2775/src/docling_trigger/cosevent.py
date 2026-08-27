"""Reading an object out of a COS notification, whatever shape it arrives in.

Code Engine subscribes the app to the bucket and delivers a CloudEvent. The
body that actually arrives from the COS event source looks like this (captured
verbatim from a request dumper)::

    headers: Ce-Type: com.ibm.cloud.cos.document.write
             Ce-Subject: 1016445.pdf
             Ce-Source: https://cloud.ibm.com/.../bucket-7q9bwndejrlvwc1
    body:    {"bucket": "bucket-7q9bwndejrlvwc1", "endpoint": "", "key": "1016445.pdf",
              "operation": "Object:Write",
              "notification": {"object_name": "1016445.pdf", "object_etag": "3cf3a4...",
                               "object_length": "128618", "event_type": "Object:Write", ...}}

Three details from that sample drive this module:

* **the same fact appears in three places** — ``key``, ``notification.object_name``
  and ``Ce-Subject`` — and they are read in that order, so a delivery missing any
  one of them still works;
* **``endpoint`` is empty.** The event does not say which S3 endpoint the bucket
  is on, so that is the deployment's configuration, not the event's;
* **``operation`` is the thing to filter on.** A bucket subscription delivers
  deletes too, and converting a deleted object is a guaranteed 404.

A structured CloudEvent (``application/cloudevents+json``, the whole envelope in
the body with the notification under ``data``) is unwrapped as well, since which
of the two shapes arrives depends on how the subscription was created.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

# Both spellings of the same fact: the COS notification's own ``event_type``
# and the CloudEvent ``type`` the Code Engine source stamps on the request.
WRITE_MARKERS = ("object:write", "document.write", "object:post", "object:put")


@dataclass(frozen=True)
class ObjectEvent:
    """One object in one bucket, plus what is worth logging about it."""

    bucket: str
    key: str
    operation: str
    endpoint: str = ""
    etag: str = ""
    length: int = 0
    content_type: str = ""
    event_id: str = ""

    @property
    def name(self) -> str:
        """The object's basename — what the document ends up being called."""
        return self.key.rsplit("/", 1)[-1]

    @property
    def is_write(self) -> bool:
        """Was an object created or overwritten? Deletes are dropped, not converted."""
        return any(marker in self.operation.lower() for marker in WRITE_MARKERS)

    def dedup_key(self) -> str:
        """Identity of a *delivery*, for suppressing an immediate redelivery.

        The etag is in it on purpose: re-uploading a changed file under the same
        key is a new document and must be converted again.
        """
        return f"{self.bucket}/{self.key}#{self.etag or self.event_id}"


def _get(mapping: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, (str, int)) and str(value).strip():
            return str(value).strip()
    return ""


def _headers(headers: Mapping[str, str]) -> dict[str, str]:
    return {str(k).lower(): str(v) for k, v in headers.items()}


def unwrap(body: Any) -> dict:
    """The notification payload, from either delivery shape.

    A structured CloudEvent carries it under ``data``; the binary encoding the
    Code Engine COS source uses puts it in the body directly, with the envelope
    in ``Ce-*`` headers.
    """
    if not isinstance(body, dict):
        return {}
    data = body.get("data")
    if isinstance(data, dict) and ("bucket" in data or "key" in data or "notification" in data):
        return data
    return body


def parse(body: Any, headers: Mapping[str, str] | None = None) -> ObjectEvent:
    """An :class:`ObjectEvent` from the request, or ``ValueError`` if there is none.

    Raising is the right answer for a body with no object in it: it is not an
    event this app can act on, and answering 400 makes that visible in the
    subscription's delivery log instead of silently succeeding.
    """
    head = _headers(headers or {})
    payload = unwrap(body)
    note = payload.get("notification") if isinstance(payload.get("notification"), dict) else {}

    bucket = _get(payload, "bucket") or _get(note, "bucket_name") or _bucket_from_source(head.get("ce-source", ""))
    key = _get(payload, "key") or _get(note, "object_name") or head.get("ce-subject", "").strip()
    operation = (
        _get(payload, "operation")
        or _get(note, "event_type")
        or head.get("ce-type", "").strip()
    )
    if not bucket or not key:
        raise ValueError("no object in this request: neither the body nor the Ce-* headers name a bucket and a key")

    raw_length = _get(note, "object_length") or _get(payload, "object_length")
    return ObjectEvent(
        bucket=bucket,
        key=key,
        operation=operation or "unknown",
        endpoint=_get(payload, "endpoint").replace("https://", "").replace("http://", "").rstrip("/"),
        etag=_get(note, "object_etag") or _get(payload, "etag"),
        length=int(raw_length) if raw_length.isdigit() else 0,
        content_type=_get(note, "content_type") or _get(payload, "content_type"),
        event_id=head.get("ce-id", "") or _get(note, "request_id"),
    )


def _bucket_from_source(ce_source: str) -> str:
    """Last resort: the bucket name is the tail of the ``Ce-Source`` catalog URL."""
    return ce_source.rstrip("/").rsplit("/", 1)[-1] if ce_source else ""


def accepts(key: str, suffixes: tuple[str, ...]) -> bool:
    """Is this object one Docling can convert? Empty ``suffixes`` accepts everything."""
    if not suffixes:
        return True
    name = key.rsplit("/", 1)[-1].lower()
    return any(name.endswith(suffix) for suffix in suffixes)
