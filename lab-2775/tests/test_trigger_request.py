"""The Docling request the trigger builds for one uploaded object.

What matters here is that it is the same request ``scripts/saas_ingest.py``
makes — the chunker discriminator present, the structure fields asked for, the
Kafka target authenticated — with the topic, and only the topic, coming from
the student.
"""

from __future__ import annotations

import base64

import pytest

from docling_trigger import cosevent, submit
from docling_trigger.settings import Env, JobSpec, ca_cert_b64

EVENT = cosevent.ObjectEvent(bucket="ws-07-docs", key="inbox/report.pdf", operation="Object:Write", etag="abc")
ENV = Env(
    bootstrap=["kafka-0:9094", "kafka-1:9094"],
    kafka_user="api-key",
    kafka_password="api-secret",
    cos_endpoint="s3.ca-tor.cloud-object-storage.appdomain.cloud",
    cos_access_key="ak",
    cos_secret_key="sk",
)
SPEC = JobSpec(docling_url="https://api.dcls.saas.ibm.com", api_key="dk", topic="ws.07.chunks", student="07")


def test_the_chunker_discriminator_is_always_present():
    # The one field the SDK drops and the server 422s without.
    assert submit.convert_options(ENV)["chunking_options"]["chunker"] == "hybrid"


def test_chunks_are_sized_for_the_embedding_model():
    options = submit.convert_options(ENV)["chunking_options"]
    assert options["tokenizer"] == ENV.tokenizer and options["max_tokens"] == ENV.max_tokens


def test_the_target_is_the_students_topic_on_the_shared_cluster():
    target = submit.kafka_target(ENV, SPEC.topic)
    assert target["kind"] == "kafka_chunks"
    assert target["topic"] == "ws.07.chunks"
    assert target["bootstrap_servers"] == ["kafka-0:9094", "kafka-1:9094"]
    assert target["key_mode"] == "doc_id"  # one document, one partition, in order
    assert target["auth"] == {"kind": "sasl", "mechanism": "PLAIN", "username": "api-key", "password": "api-secret"}
    assert target["security_protocol"] == "SASL_SSL"
    # Opt-in on the service, and every stage that cites a page needs them.
    assert target["headings_field"] == "headings" and target["page_field"] == "page_numbers"


# A real (expired, lab) CA, trimmed to two lines of body — enough to be a PEM.
PEM = (
    "-----BEGIN CERTIFICATE-----\n"
    "MIIFDDCCAvSgAwIBAgIQEyFeg1CeiVu0pFwihzP7+zANBgkqhkiG9w0BAQ0FADAg\n"
    "-----END CERTIFICATE-----\n"
)
PEM_B64 = base64.b64encode(PEM.encode()).decode()


def test_the_ca_certificate_rides_in_the_auth_block_as_base64():
    # What lets Docling verify a privately signed broker instead of skipping
    # verification altogether.
    env = Env(**{**ENV.__dict__, "kafka_ca_cert": PEM_B64, "kafka_verify_certs": True})
    target = submit.kafka_target(env, "ws.07.chunks")
    assert target["auth"]["ca_cert"] == PEM_B64
    assert target["verify_certs"] is True
    assert base64.b64decode(target["auth"]["ca_cert"]).startswith(b"-----BEGIN CERTIFICATE-----")


def test_no_ca_means_no_field_at_all():
    assert "ca_cert" not in submit.kafka_target(ENV, "ws.07.chunks")["auth"]


def test_the_ca_is_summarised_in_a_log_line_not_dumped():
    env = Env(**{**ENV.__dict__, "kafka_ca_cert": PEM_B64})
    redacted = submit.redact(submit.build_request(env, SPEC, EVENT))
    assert redacted["target"]["auth"]["ca_cert"] == f"<{len(PEM_B64)} bytes of base64 PEM>"


def test_a_pem_and_its_base64_are_both_accepted():
    assert ca_cert_b64(PEM) == PEM_B64
    assert ca_cert_b64(PEM_B64) == PEM_B64  # already encoded: passed through
    assert ca_cert_b64("  ") == ""


def test_a_ca_that_is_neither_is_rejected_rather_than_sent():
    # Silence here would surface as a TLS handshake failure inside Docling,
    # which this app never gets to see.
    for bad in ("not base64 at all!!", base64.b64encode(b"hello").decode()):
        with pytest.raises(ValueError):
            ca_cert_b64(bad)


def test_verification_follows_the_ca_unless_it_is_overridden(monkeypatch):
    monkeypatch.setenv("KAFKA_CA_CERT_B64", PEM_B64)
    monkeypatch.delenv("KAFKA_VERIFY_CERTS", raising=False)
    assert Env.from_environ().kafka_verify_certs is True
    monkeypatch.setenv("KAFKA_VERIFY_CERTS", "false")
    assert Env.from_environ().kafka_verify_certs is False
    # Empty means unset, not false — deploy.sh passes every variable through.
    monkeypatch.setenv("KAFKA_VERIFY_CERTS", "")
    assert Env.from_environ().kafka_verify_certs is True


def test_a_ca_can_also_be_a_file_on_disk(monkeypatch, tmp_path):
    path = tmp_path / "kafka-ca.crt"
    path.write_text(PEM)
    monkeypatch.delenv("KAFKA_CA_CERT_B64", raising=False)
    monkeypatch.setenv("KAFKA_CA_LOCATION", str(path))
    assert Env.from_environ().kafka_ca_cert == PEM_B64


def test_a_broken_ca_is_reported_and_does_not_stop_the_app(monkeypatch):
    monkeypatch.setenv("KAFKA_CA_CERT_B64", "not base64 at all!!")
    env = Env.from_environ()  # must not raise: the app has to start and say so
    assert env.kafka_ca_cert == ""
    assert any("CA certificate" in p for p in env.problems())


def test_a_cluster_without_credentials_is_plaintext():
    target = submit.kafka_target(Env(bootstrap=["localhost:29092"]), "docling.chunks")
    assert target["security_protocol"] == "PLAINTEXT" and "auth" not in target


def test_the_default_source_is_a_presigned_url_for_exactly_that_object():
    source = submit.source_for(ENV, EVENT)
    assert source["kind"] == "http"
    assert source["url"].startswith(
        "https://s3.ca-tor.cloud-object-storage.appdomain.cloud/ws-07-docs/inbox/report.pdf?"
    )
    assert "X-Amz-Signature=" in source["url"]


def test_the_s3_mode_hands_docling_the_coordinates_instead():
    source = submit.source_for(Env(**{**ENV.__dict__, "source_mode": "s3"}), EVENT)
    assert source["kind"] == "s3"
    assert (source["bucket"], source["key_prefix"]) == ("ws-07-docs", "inbox/report.pdf")
    # key_prefix is a prefix: cap what a near-miss key can drag in.
    assert source["max_num_elements"] == 1


def test_an_endpoint_named_by_the_event_wins_over_the_deployments():
    event = cosevent.ObjectEvent(bucket="b", key="k.pdf", operation="Object:Write", endpoint="s3.eu-de.example")
    assert submit.endpoint_for(ENV, event) == "s3.eu-de.example"


def test_the_endpoint_can_be_derived_from_the_region_alone():
    env = Env(**{**ENV.__dict__, "cos_endpoint": "", "cos_region": "us-south"})
    assert submit.endpoint_for(env, EVENT) == "s3.us-south.cloud-object-storage.appdomain.cloud"


def test_the_whole_request_is_one_source_and_one_target():
    payload = submit.build_request(ENV, SPEC, EVENT)
    assert set(payload) == {"options", "sources", "target"}
    assert len(payload["sources"]) == 1
    assert submit.submit_url(SPEC.docling_url) == "https://api.dcls.saas.ibm.com/v1/convert/source/batch"
    assert submit.auth_headers(SPEC) == {"X-Api-Key": "dk"}


def test_nothing_secret_survives_redaction():
    redacted = submit.redact(submit.build_request(ENV, SPEC, EVENT))
    printed = str(redacted)
    assert "api-secret" not in printed and "X-Amz-Signature" not in printed
    assert redacted["target"]["auth"]["password"] == "***"
    # The redacted request still shows which object it was for.
    assert "/ws-07-docs/inbox/report.pdf" in redacted["sources"][0]["url"]


def test_the_s3_secret_is_redacted_too():
    payload = submit.build_request(Env(**{**ENV.__dict__, "source_mode": "s3"}), SPEC, EVENT)
    assert submit.redact(payload)["sources"][0]["secret_key"] == "***"


def test_a_deployment_with_no_cos_credentials_is_reported_not_guessed():
    problems = Env(bootstrap=["k:9094"], kafka_user="u", kafka_password="p").problems()
    assert any("COS_ACCESS_KEY_ID" in p for p in problems)
