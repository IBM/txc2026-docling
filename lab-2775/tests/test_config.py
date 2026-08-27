"""Configuration that is easy to get wrong and expensive to get wrong late.

Both cases here failed a running job rather than a submit: a SASL job that
starts and then dies on "No LoginModule found", and a SINK_TYPE typo that
silently selected the discard sink.
"""

from __future__ import annotations

import pytest

from pipeline.config import KafkaConfig, OpenSearchConfig, sink_type_from_env, sink_types_from_env


@pytest.fixture(autouse=True)
def _kafka_env(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    monkeypatch.setenv("KAFKA_API_KEY", "user")
    monkeypatch.setenv("KAFKA_API_SECRET", "secret")
    monkeypatch.delenv("SINK_TYPE", raising=False)
    monkeypatch.delenv("KAFKA_SASL_LOGIN_MODULE", raising=False)
    monkeypatch.delenv("KAFKA_CA_LOCATION", raising=False)
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")


def test_jaas_names_the_shaded_login_module():
    """flink-sql-connector-kafka relocates kafka-clients, so the unshaded class
    is not on the classpath and the job dies at start."""
    jaas = KafkaConfig().sasl_jaas_config
    assert jaas.startswith("org.apache.flink.kafka.shaded.org.apache.kafka.")
    assert "PlainLoginModule required" in jaas
    assert 'username="user"' in jaas


def test_scram_swaps_the_module_but_keeps_the_shading(monkeypatch):
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
    jaas = KafkaConfig().sasl_jaas_config
    assert "security.scram.ScramLoginModule" in jaas
    assert jaas.startswith("org.apache.flink.kafka.shaded.")


def test_login_module_is_overridable(monkeypatch):
    monkeypatch.setenv("KAFKA_SASL_LOGIN_MODULE", "com.example.MyLoginModule")
    assert KafkaConfig().sasl_jaas_config.startswith("com.example.MyLoginModule required")


def test_sasl_without_credentials_fails_loudly(monkeypatch):
    monkeypatch.setenv("KAFKA_API_SECRET", "")
    with pytest.raises(RuntimeError, match="KAFKA_API_KEY"):
        KafkaConfig().properties()


def test_ca_becomes_a_pem_truststore(monkeypatch):
    monkeypatch.setenv("KAFKA_CA_LOCATION", "/certs/ca.crt")
    props = KafkaConfig().properties()
    assert props["ssl.truststore.type"] == "PEM"
    assert props["ssl.truststore.location"] == "/certs/ca.crt"
    assert props["ssl.endpoint.identification.algorithm"] == "https"


def test_plaintext_carries_no_security_properties(monkeypatch):
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    props = KafkaConfig().properties()
    assert props["security.protocol"] == "PLAINTEXT"
    assert not [k for k in props if k.startswith(("sasl.", "ssl."))]


def test_sink_type_default_is_kafka():
    assert sink_type_from_env() == "kafka"


def test_unknown_sink_type_is_rejected_at_submit(monkeypatch):
    monkeypatch.setenv("SINK_TYPE", "opensearh")
    with pytest.raises(ValueError, match="SINK_TYPE"):
        sink_type_from_env()


# --- SINK_TYPE names a list ------------------------------------------------
# The workshop writes the output topic *and* the OpenSearch index, so the
# finished record has two destinations and the parser has to say so. Every case
# below is one a typo produces, and each used to be a job that ran happily
# while writing somewhere nobody was looking.

def test_sink_types_default_is_kafka_alone():
    assert sink_types_from_env() == ("kafka",)


def test_sink_types_reads_a_list(monkeypatch):
    monkeypatch.setenv("SINK_TYPE", "kafka,opensearch")
    assert sink_types_from_env() == ("kafka", "opensearch")


@pytest.mark.parametrize("raw", ["kafka + opensearch", "kafka opensearch", " KAFKA, OpenSearch "])
def test_sink_types_separators_and_case(monkeypatch, raw):
    monkeypatch.setenv("SINK_TYPE", raw)
    assert sink_types_from_env() == ("kafka", "opensearch")


def test_sink_types_keeps_order_and_drops_repeats(monkeypatch):
    """Two `kafka` entries would attach the sink twice and double-write the topic."""
    monkeypatch.setenv("SINK_TYPE", "opensearch,kafka,opensearch")
    assert sink_types_from_env() == ("opensearch", "kafka")


def test_none_refuses_to_be_combined(monkeypatch):
    monkeypatch.setenv("SINK_TYPE", "none,kafka")
    with pytest.raises(ValueError, match="none"):
        sink_types_from_env()


def test_one_bad_name_in_a_list_is_still_rejected(monkeypatch):
    monkeypatch.setenv("SINK_TYPE", "kafka,opensearh")
    with pytest.raises(ValueError, match="opensearh"):
        sink_types_from_env()


def test_empty_sink_type_is_rejected(monkeypatch):
    monkeypatch.setenv("SINK_TYPE", "   ")
    with pytest.raises(ValueError, match="empty"):
        sink_types_from_env()


def test_sink_type_display_joins_the_list(monkeypatch):
    monkeypatch.setenv("SINK_TYPE", "kafka,opensearch")
    assert sink_type_from_env() == "kafka+opensearch"


# --- OpenSearch: the failures that look like a healthy pipeline ------------

@pytest.fixture
def _os_env(monkeypatch):
    monkeypatch.setenv("OPENSEARCH_HOSTS", "https://os.example:9200")
    monkeypatch.setenv("OPENSEARCH_INDEX", "student07-chunks")
    monkeypatch.setenv("OPENSEARCH_USERNAME", "student07")
    monkeypatch.setenv("OPENSEARCH_PASSWORD", "pw")
    monkeypatch.delenv("OPENSEARCH_CA_LOCATION", raising=False)
    monkeypatch.setenv("OPENSEARCH_VERIFY_CERTS", "true")


def test_opensearch_require_accepts_the_students_own_index(_os_env):
    OpenSearchConfig().require()


def test_index_outside_the_students_namespace_is_rejected(_os_env, monkeypatch):
    """The cluster grants studentNN rights on studentNN-* and nothing else, so
    this is a 403 on every write from a job that otherwise looks healthy."""
    monkeypatch.setenv("OPENSEARCH_INDEX", "document-chunks")
    with pytest.raises(RuntimeError, match="namespace"):
        OpenSearchConfig().require()


def test_missing_password_is_rejected_at_submit(_os_env, monkeypatch):
    monkeypatch.setenv("OPENSEARCH_PASSWORD", "")
    with pytest.raises(RuntimeError, match="OPENSEARCH_PASSWORD"):
        OpenSearchConfig().require()


def test_a_ca_path_that_is_not_there_is_rejected(_os_env, monkeypatch):
    """In the cluster the CA is mounted from the workshop Secret; a missing
    mount is an SSL handshake failure on the first write, attributed to nothing."""
    monkeypatch.setenv("OPENSEARCH_CA_LOCATION", "/no/such/ca.pem")
    with pytest.raises(RuntimeError, match="OPENSEARCH_CA_LOCATION"):
        OpenSearchConfig().require()


def test_ca_reaches_the_client(_os_env, monkeypatch, tmp_path):
    ca = tmp_path / "ca.pem"
    ca.write_text("-----BEGIN CERTIFICATE-----\n")
    monkeypatch.setenv("OPENSEARCH_CA_LOCATION", str(ca))
    kwargs = OpenSearchConfig().client_kwargs()
    assert kwargs["ca_certs"] == str(ca)
    assert kwargs["verify_certs"] is True
    assert kwargs["http_auth"] == ("student07", "pw")


def test_no_ca_key_when_none_is_configured(_os_env):
    assert "ca_certs" not in OpenSearchConfig().client_kwargs()
