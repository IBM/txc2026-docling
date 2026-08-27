"""The HTTP surface: what each kind of delivery is answered with.

Skipped wherever ``fastapi`` is not installed — it is the trigger image's
dependency, not the repo's, the same way the pyflink tests are skipped outside
the pipeline image. Everything the app *decides* lives in the pure modules and
is tested without it.
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient  # noqa: E402

from test_trigger_event import BODY, HEADERS  # noqa: E402

ENVIRONMENT = {
    "KAFKA_BOOTSTRAP_SERVERS": "kafka-0:9094,kafka-1:9094",
    "KAFKA_API_KEY": "api-key",
    "KAFKA_API_SECRET": "api-secret",
    "COS_ACCESS_KEY_ID": "ak",
    "COS_SECRET_ACCESS_KEY": "sk",
    "COS_BUCKET_REGION": "ca-tor",
    "TRIGGER_DEDUP_TTL_S": "300",
}
STUDENT = {
    "X-Docling-Url": "https://api.dcls.saas.ibm.com",
    "X-Docling-Api-Key": "dk",
    "X-Kafka-Topic": "ws.07.chunks",
    "X-Student-Id": "07",
}


@pytest.fixture()
def client(monkeypatch):
    for key, value in ENVIRONMENT.items():
        monkeypatch.setenv(key, value)
    from docling_trigger import app as app_module

    with TestClient(app_module.app) as client:
        yield client


def post(client, headers=None, body=None, path="/"):
    return client.post(path, content=json.dumps(body or BODY), headers={**HEADERS, **(headers or {})})


def test_a_write_event_is_answered_with_what_would_be_submitted(client):
    reply = post(client, {**STUDENT, "X-Dry-Run": "1"})
    assert reply.status_code == 200
    request = reply.json()["request"]
    assert request["target"]["topic"] == "ws.07.chunks"
    assert request["sources"][0]["url"].startswith(
        "https://s3.ca-tor.cloud-object-storage.appdomain.cloud/bucket-7q9bwndejrlvwc1/1016445.pdf"
    )
    assert request["target"]["auth"]["password"] == "***"


def test_every_path_is_a_valid_place_to_post_an_event(client):
    # Subscriptions in the wild post to /, /callback and /events.
    for path in ("/", "/callback", "/events/cos"):
        assert post(client, {**STUDENT, "X-Dry-Run": "1"}, path=path).status_code == 200


def test_a_delete_is_acknowledged_and_ignored(client):
    reply = post(client, STUDENT, body={**BODY, "operation": "Object:Delete"})
    # 200, not an error: a retry would deliver the same delete forever.
    assert reply.status_code == 200 and reply.json()["ignored"] == "not an object write"


def test_a_non_document_is_acknowledged_and_ignored(client):
    reply = client.post(
        "/",
        content=json.dumps({**BODY, "key": ".DS_Store"}),
        headers={**HEADERS, **STUDENT, "Ce-Subject": ".DS_Store"},
    )
    assert reply.status_code == 200 and "not a convertible document" in reply.text


def test_a_missing_topic_header_is_a_400_naming_the_header(client):
    reply = post(client, {"X-Docling-Url": "https://api.dcls.saas.ibm.com"})
    assert reply.status_code == 400 and "X-Kafka-Topic" in reply.text


def test_a_body_with_no_object_is_a_400(client):
    reply = client.post("/", content=json.dumps({"hello": "world"}))
    assert reply.status_code == 400


def test_docling_being_unreachable_is_a_502_so_the_delivery_can_be_retried(client, monkeypatch):
    import httpx

    from docling_trigger import app as app_module

    async def refuse(*args, **kwargs):
        raise httpx.ConnectError("nope")

    monkeypatch.setattr(app_module.app.state.client, "post", refuse)
    assert post(client, STUDENT).status_code == 502


def test_a_successful_submission_returns_the_task_id(client, monkeypatch):
    import httpx

    from docling_trigger import app as app_module

    seen = {}

    async def accept(url, json=None, headers=None):
        seen.update(url=url, payload=json, headers=headers)
        return httpx.Response(200, json={"task_id": "t-1", "task_status": "pending"})

    monkeypatch.setattr(app_module.app.state.client, "post", accept)
    reply = post(client, STUDENT)
    assert reply.status_code == 202
    assert reply.json()["task_id"] == "t-1" and reply.json()["topic"] == "ws.07.chunks"
    assert seen["url"] == "https://api.dcls.saas.ibm.com/v1/convert/source/batch"
    assert seen["headers"] == {"X-Api-Key": "dk"}
    assert seen["payload"]["options"]["chunking_options"]["chunker"] == "hybrid"


def test_a_rejected_submission_is_a_502_carrying_doclings_own_reason(client, monkeypatch):
    import httpx

    from docling_trigger import app as app_module

    async def reject(*args, **kwargs):
        return httpx.Response(422, text="Unable to extract tag using discriminator")

    monkeypatch.setattr(app_module.app.state.client, "post", reject)
    reply = post(client, STUDENT)
    assert reply.status_code == 502 and "discriminator" in reply.json()["detail"]


def test_a_redelivery_is_suppressed_but_a_dry_run_never_causes_one(client, monkeypatch):
    import httpx

    from docling_trigger import app as app_module

    calls = []

    async def accept(url, json=None, headers=None):
        calls.append(url)
        return httpx.Response(200, json={"task_id": "t-1", "task_status": "pending"})

    monkeypatch.setattr(app_module.app.state.client, "post", accept)
    # Checking the configuration must not consume the upload that follows it.
    assert post(client, {**STUDENT, "X-Dry-Run": "1"}).status_code == 200
    assert post(client, STUDENT).status_code == 202
    assert post(client, STUDENT).json()["ignored"] == "duplicate delivery"
    assert len(calls) == 1


def test_the_root_page_tells_a_student_what_to_configure(client):
    page = client.get("/").text
    assert "chunkstopic" in page and "X-Docling-Url" in page
    assert "Configuration: ok." in page


def test_health_is_ok_even_when_the_deployment_is_misconfigured(monkeypatch):
    from docling_trigger import app as app_module

    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.delenv("COS_ACCESS_KEY_ID", raising=False)
    with TestClient(app_module.app) as client:
        reply = client.get("/health")
    assert reply.status_code == 200
    assert any("KAFKA_BOOTSTRAP_SERVERS" in p for p in reply.json()["problems"])


def test_a_redelivery_within_the_ttl_is_suppressed(monkeypatch):
    from docling_trigger.app import Recent

    recent = Recent(ttl=60)
    assert not recent.hit("a", now=0) and recent.hit("a", now=1)
    assert not recent.hit("a", now=100)  # the ttl has passed: convert it again
