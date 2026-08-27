"""Reading a COS notification, and deciding whether to act on it.

The bodies here are the shapes that actually arrive: the one captured verbatim
from the Code Engine COS event source, the structured CloudEvent envelope, and
the deliveries that are missing one field or another.
"""

from __future__ import annotations

import pytest

from docling_trigger import cosevent
from docling_trigger.settings import Env, JobSpec

# Captured from a request dumper subscribed to a real bucket.
BODY = {
    "bucket": "bucket-7q9bwndejrlvwc1",
    "endpoint": "",
    "key": "1016445.pdf",
    "notification": {
        "bucket_name": "bucket-7q9bwndejrlvwc1",
        "content_type": "application/pdf",
        "event_type": "Object:Write",
        "format": "2.0",
        "object_etag": "3cf3a49dfe06b198da72560cb3456ada",
        "object_length": "128618",
        "object_name": "1016445.pdf",
        "request_id": "1c7249c8-0eca-4cfc-8392-ac7d416bc257",
        "request_time": "2026-08-13T11:56:31.032Z",
    },
    "operation": "Object:Write",
}
HEADERS = {
    "Ce-Id": "1c7249c8-0eca-4cfc-8392-ac7d416bc257",
    "Ce-Source": "https://cloud.ibm.com/catalog/services/cloud-object-storage/bucket-7q9bwndejrlvwc1",
    "Ce-Subject": "1016445.pdf",
    "Ce-Type": "com.ibm.cloud.cos.document.write",
}


def test_the_captured_event_parses():
    event = cosevent.parse(BODY, HEADERS)
    assert (event.bucket, event.key) == ("bucket-7q9bwndejrlvwc1", "1016445.pdf")
    assert event.is_write and event.length == 128618
    assert event.etag == "3cf3a49dfe06b198da72560cb3456ada"
    assert event.endpoint == ""  # the event does not say; configuration does


def test_the_headers_alone_are_enough():
    # A delivery whose body this app does not recognise still names the object
    # in its CloudEvent headers.
    event = cosevent.parse({"something": "else"}, HEADERS)
    assert (event.bucket, event.key) == ("bucket-7q9bwndejrlvwc1", "1016445.pdf")
    assert event.is_write  # from Ce-Type


def test_a_structured_cloudevent_is_unwrapped():
    event = cosevent.parse({"specversion": "1.0", "type": "com.ibm.cloud.cos.document.write", "data": BODY}, {})
    assert event.key == "1016445.pdf" and event.is_write


def test_a_delete_is_parsed_but_not_a_write():
    body = {**BODY, "operation": "Object:Delete"}
    assert not cosevent.parse(body, {}).is_write


def test_a_request_with_no_object_is_rejected():
    with pytest.raises(ValueError):
        cosevent.parse({"hello": "world"}, {})


def test_the_dedup_key_changes_when_the_file_does():
    same = cosevent.parse(BODY, HEADERS).dedup_key()
    changed = {**BODY, "notification": {**BODY["notification"], "object_etag": "0000"}}
    assert cosevent.parse(changed, HEADERS).dedup_key() != same


def test_only_convertible_suffixes_are_accepted():
    suffixes = Env().suffixes or (".pdf", ".docx")
    assert cosevent.accepts("a/b/report.PDF", suffixes)
    assert not cosevent.accepts(".DS_Store", suffixes)
    # An empty filter is the escape hatch, and accepts anything.
    assert cosevent.accepts(".DS_Store", ())


def test_keys_with_a_prefix_keep_their_basename():
    event = cosevent.parse({**BODY, "key": "inbox/2026/report.pdf"}, {})
    assert event.name == "report.pdf" and event.key == "inbox/2026/report.pdf"


# --- the three per-student values -----------------------------------------
ENV = Env(default_topic="", docling_url="https://shared.example", docling_api_key="shared-key")


def test_plain_headers_are_read():
    spec = JobSpec.from_headers(
        {"X-Docling-Url": "https://mine.example/", "X-Docling-Api-Key": "k", "X-Kafka-Topic": "ws.07.chunks"}, ENV
    )
    assert spec.docling_url == "https://mine.example"  # trailing slash trimmed
    assert (spec.api_key, spec.topic) == ("k", "ws.07.chunks")
    assert spec.problems() == []


def test_cloudevent_extensions_are_read_the_same_way():
    # What `ibmcloud ce sub cos create --extension chunkstopic=...` delivers.
    spec = JobSpec.from_headers(
        {"Ce-Doclingurl": "https://mine.example", "Ce-Doclingkey": "k", "Ce-Chunkstopic": "ws.07.chunks"}, ENV
    )
    assert (spec.docling_url, spec.api_key, spec.topic) == ("https://mine.example", "k", "ws.07.chunks")


def test_a_structured_events_attributes_are_read_too():
    spec = JobSpec.from_headers({}, ENV, attributes={"chunkstopic": "ws.09.chunks", "studentid": "09"})
    assert (spec.topic, spec.student) == ("ws.09.chunks", "09")


def test_headers_win_over_the_deployments_defaults():
    spec = JobSpec.from_headers({"X-Kafka-Topic": "ws.07.chunks"}, ENV)
    assert spec.docling_url == "https://shared.example" and spec.api_key == "shared-key"


def test_a_missing_topic_is_an_error_and_says_which_header():
    problems = JobSpec.from_headers({}, ENV).problems()
    assert any("X-Kafka-Topic" in p and "chunkstopic" in p for p in problems)


def test_a_student_id_can_stand_in_for_the_topic():
    env = Env(topic_template="ws.{id}.chunks", docling_url="https://shared.example")
    assert JobSpec.from_headers({"X-Student-Id": "07"}, env).topic == "ws.07.chunks"
