"""Runtime configuration, read from the environment and nowhere else.

The same image runs both workshop pipelines and every environment they are
deployed to; what differs is a handful of variables, in five families:

    KAFKA_*       connection and the five topics (:class:`KafkaConfig`)
    PIPELINE_*    the stage knobs both jobs share (:class:`PipelineConfig`)
    EMBEDDING_*   which vectors to ask for (:class:`EmbeddingConfig`)
    WATSONX_*     who to ask (:class:`WatsonxConfig`)
    OPENSEARCH_*  where SINK_TYPE=opensearch writes (:class:`OpenSearchConfig`)
    WATSONX_LLM_* the model the dashboard's RAG tab answers with
                  (:class:`GenerationConfig`) — no job uses it

Secrets — the Kafka API secret, ``WATSONX_APIKEY``, the OpenSearch password —
are injected by the orchestrator (a Kubernetes Secret referenced from the
FlinkApplication's podTemplate), never baked into the image.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def _flag(name: str, default: bool) -> bool:
    return _env(name, "true" if default else "false").strip().lower() == "true"


def _int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


# Terminal stages, chosen with SINK_TYPE — and it names a *list*, because the
# finished record usefully goes to more than one place at once:
#   kafka      — write the finished record to KAFKA_OUTPUT_TOPIC; read it back
#                with scripts/drain_topic.py. Needs no storage of its own.
#   opensearch — index the chunk + vector (needs a reachable cluster)
#   log        — one summary line per chunk in the TaskManager log
#   none       — count and drop; cannot be combined with anything
SINK_TYPES = ("kafka", "opensearch", "log", "none")

# The workshop's default, and the reason SINK_TYPE stopped being one word: a
# student wants both halves of the ending. The topic is what `drain_topic.py`
# prints and what makes the enrichment legible record by record; the index is
# what the dashboard's Ask tab retrieves from. Choosing between them used to
# mean choosing which half of the lab to see.
DEFAULT_SINK_TYPES = "kafka,opensearch"


def sink_types_from_env(default: str = "kafka") -> tuple[str, ...]:
    """``SINK_TYPE`` as the tuple of terminal stages to attach.

    Accepts one name or several (``kafka,opensearch`` — ``+`` and whitespace
    separate too), validated here at submit time rather than mid-stream: a
    typo in a sink name should stop the job before it consumes a record, not
    at the first one.
    """
    raw = _env("SINK_TYPE", default)
    names = [n for n in re.split(r"[,+\s]+", raw.strip().lower()) if n]
    if not names:
        raise ValueError("SINK_TYPE is empty — set it to one or more of " + ", ".join(SINK_TYPES))

    unknown = [n for n in names if n not in SINK_TYPES]
    if unknown:
        raise ValueError(
            f"SINK_TYPE names {', '.join(repr(u) for u in unknown)}, which is not a sink; "
            f"choose from {', '.join(SINK_TYPES)} (several, comma-separated, is allowed)"
        )
    # De-duplicated, in the order they were written: two `kafka` entries would
    # otherwise attach the same sink twice and double-write the topic.
    ordered = tuple(dict.fromkeys(names))
    if "none" in ordered and len(ordered) > 1:
        raise ValueError(
            "SINK_TYPE=none means 'write nothing at all', so it cannot be combined "
            f"with {', '.join(n for n in ordered if n != 'none')}"
        )
    return ordered


def sink_type_from_env(default: str = "kafka") -> str:
    """``SINK_TYPE`` rendered for display — the list, joined back up.

    The jobs wire themselves from :func:`sink_types_from_env`; this is for the
    log line at submit and for the dashboard's sink label.
    """
    return "+".join(sink_types_from_env(default))


@dataclass(frozen=True)
class KafkaConfig:
    """Connection and topics.

    Three connection shapes, selected by ``KAFKA_SECURITY_PROTOCOL``:
      * ``PLAINTEXT`` — local broker (docker/podman) and the in-cluster listener
        a CMF-deployed job talks to; no credentials.
      * ``SASL_SSL``  — the hosted Confluent brokers and Confluent Cloud: PLAIN
        auth with API key + secret. The lab brokers present a certificate from a
        private CA, so ``KAFKA_CA_LOCATION`` must point at that CA — Flink's
        Kafka client is the Java one, which has no "skip verification" switch
        the way librdkafka does.
      * ``SASL_PLAINTEXT`` — SASL inside a trusted network, no TLS.

    Five topics, and both jobs use the same names: the simple pipeline reads
    ``chunk_topic`` and writes ``output_topic``; the full one adds the three the
    guard needs. Everything is per-student in the workshop, so these arrive
    already namespaced (``ws.07.chunks``) by :mod:`labtools.config`.
    """

    bootstrap_servers: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP_SERVERS"))

    # What Docling's kafka_chunks target writes — the entry point of both jobs.
    chunk_topic: str = field(default_factory=lambda: _env("KAFKA_CHUNKS_TOPIC", "docling.chunks"))
    # Where SINK_TYPE=kafka puts the finished records.
    output_topic: str = field(default_factory=lambda: _env("KAFKA_OUTPUT_TOPIC", "docling.chunks.enriched"))
    # The control plane, and the two audit trails the guard forks into.
    policy_topic: str = field(default_factory=lambda: _env("KAFKA_POLICY_TOPIC", "policy-rules"))
    quarantine_topic: str = field(default_factory=lambda: _env("KAFKA_QUARANTINE_TOPIC", "pii-quarantine"))
    rejected_topic: str = field(default_factory=lambda: _env("KAFKA_REJECTED_TOPIC", "quality-rejected"))
    # One group per student. Two students sharing a group on one topic is the
    # silent failure that eats an afternoon: Kafka splits the partitions
    # between them and most of the class sees no data at all.
    consumer_group: str = field(default_factory=lambda: _env("KAFKA_CONSUMER_GROUP", "chunk-pipeline"))

    security_protocol: str = field(default_factory=lambda: _env("KAFKA_SECURITY_PROTOCOL", "SASL_SSL").upper())
    sasl_mechanism: str = field(default_factory=lambda: _env("KAFKA_SASL_MECHANISM", "PLAIN"))
    api_key: str = field(default_factory=lambda: _env("KAFKA_API_KEY"))
    api_secret: str = field(default_factory=lambda: _env("KAFKA_API_SECRET"))
    # PEM file holding the CA that signed the broker certificates. Must be a
    # path *inside the TaskManager container*, not on the laptop.
    ca_location: str = field(default_factory=lambda: _env("KAFKA_CA_LOCATION"))
    # Set to "" (empty) to skip broker hostname verification. The lab
    # certificate carries the broker IP in its SANs, so the default holds.
    ssl_endpoint_algorithm: str = field(default_factory=lambda: _env("KAFKA_SSL_ENDPOINT_ALGORITHM", "https"))
    # The JAAS login module class, which is NOT the one you would write from
    # memory. flink-sql-connector-kafka is a fat jar that *shades* kafka-clients,
    # so the plain `org.apache.kafka.common.security.plain.PlainLoginModule` is
    # not on the classpath at all and SASL fails at job start with
    # "No LoginModule found". Override only if you swap the connector for one
    # that does not shade.
    sasl_login_module: str = field(
        default_factory=lambda: _env(
            "KAFKA_SASL_LOGIN_MODULE",
            "org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule",
        )
    )

    @property
    def is_sasl(self) -> bool:
        return self.security_protocol.startswith("SASL")

    @property
    def is_ssl(self) -> bool:
        return self.security_protocol.endswith("SSL")

    @property
    def sasl_jaas_config(self) -> str:
        if not (self.api_key and self.api_secret):
            raise RuntimeError("KAFKA_API_KEY / KAFKA_API_SECRET are required for SASL auth")
        module = self.sasl_login_module
        if self.sasl_mechanism.upper().startswith("SCRAM") and module.endswith("PlainLoginModule"):
            module = module.replace("security.plain.PlainLoginModule", "security.scram.ScramLoginModule")
        return f'{module} required username="{self.api_key}" password="{self.api_secret}";'

    def properties(self) -> dict[str, str]:
        props = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.consumer_group,
            "security.protocol": self.security_protocol,
        }
        if self.is_sasl:
            props["sasl.mechanism"] = self.sasl_mechanism
            props["sasl.jaas.config"] = self.sasl_jaas_config
        if self.is_ssl and self.ca_location:
            # A PEM truststore rather than a JKS: Kafka clients have accepted
            # `ssl.truststore.type=PEM` since 2.7, which saves generating a
            # keystore just to trust one lab CA.
            props["ssl.truststore.type"] = "PEM"
            props["ssl.truststore.location"] = self.ca_location
        if self.is_ssl:
            props["ssl.endpoint.identification.algorithm"] = self.ssl_endpoint_algorithm
        return props


@dataclass(frozen=True)
class PipelineConfig:
    """The stage knobs, shared by both jobs.

    Every optional stage can be switched off here, which is how the same image
    runs the whole graph or a cut-down version for a demo — and how a student
    inspects enrichment without spending a watsonx.ai call per chunk.
    """

    # Chunks shorter than this are flagged (and dropped when drop_low_quality).
    min_chars: int = field(default_factory=lambda: _int("PIPELINE_MIN_CHARS", 40))
    drop_low_quality: bool = field(default_factory=lambda: _flag("PIPELINE_DROP_LOW_QUALITY", False))
    # Only ``full_job`` wires the dedup stage in; ``enrich_job`` is stateless,
    # so these two do nothing when the simple pipeline is what is deployed.
    dedup: bool = field(default_factory=lambda: _flag("PIPELINE_DEDUP", True))
    dedup_ttl_hours: int = field(default_factory=lambda: _int("PIPELINE_DEDUP_TTL_HOURS", 24))
    # Embedding is the expensive stage; turn it off to inspect enrichment alone.
    embed: bool = field(default_factory=lambda: _flag("PIPELINE_EMBED", True))
    # It is also a network call, so it is batched: one request per chunk would
    # be a thousand round trips per student per run. The timer bounds how long
    # a partly-filled batch waits.
    embed_batch_size: int = field(default_factory=lambda: _int("PIPELINE_EMBED_BATCH_SIZE", 32))
    embed_batch_delay_ms: int = field(default_factory=lambda: _int("PIPELINE_EMBED_BATCH_DELAY_MS", 500))
    # Buffers for the embed stage: one per key, so each subtask gets its own
    # batch and its own timer. The key means nothing else.
    embed_shards: int = field(default_factory=lambda: _int("PIPELINE_EMBED_SHARDS", 4))
    parallelism: int = field(default_factory=lambda: _int("PIPELINE_PARALLELISM", 1))


@dataclass(frozen=True)
class EmbeddingConfig:
    """Which vectors the embed stage asks watsonx.ai for.

    ``model_id`` is a *watsonx.ai* model id, not a HuggingFace repo — the
    pipeline loads no model of its own. ``dimension`` must match what that model
    returns; :mod:`pipeline.watsonx` checks the first response and fails the job
    immediately rather than letting a mismatch reach the OpenSearch
    ``knn_vector`` mapping.
    """

    model_id: str = field(
        default_factory=lambda: _env("EMBEDDING_MODEL_ID", "ibm/granite-embedding-278m-multilingual")
    )
    # granite-embedding-278m-multilingual emits 768-dim vectors. Change both
    # together, and rebuild the index. Note that the region decides what exists
    # at all: ca-tor serves exactly four embedding models — this one and
    # slate-125m-english-rtrvr-v2 at 768, slate-30m-english-rtrvr-v2 at 384,
    # multilingual-e5-large at 1024. Asking for anything else is a 404.
    dimension: int = field(default_factory=lambda: _int("EMBEDDING_DIMENSION", 768))
    # Token budget per chunk. The producers size chunks to it (they read
    # CHUNK_TOKENIZER_ID — a *HuggingFace* repo id, not model_id above — and
    # give its tokenizer to Docling's chunker), and watsonx.ai truncates at it.
    max_tokens: int = field(default_factory=lambda: _int("EMBEDDING_MAX_TOKENS", 512))
    normalize: bool = field(default_factory=lambda: _flag("EMBEDDING_NORMALIZE", True))


@dataclass(frozen=True)
class WatsonxConfig:
    """Credentials and endpoint for watsonx.ai text embeddings.

    ``WATSONX_APIKEY`` is read lazily and validated in :meth:`require` rather
    than at import, so a job with ``PIPELINE_EMBED=false`` still starts without
    one.
    """

    # Regional, and it is not a routing detail: the catalogue of embedding
    # models differs per region, so moving this moves EMBEDDING_MODEL_ID too.
    url: str = field(default_factory=lambda: _env("WATSONX_URL", "https://ca-tor.ml.cloud.ibm.com"))
    iam_url: str = field(default_factory=lambda: _env("WATSONX_IAM_URL", "https://iam.cloud.ibm.com/identity/token"))
    api_key: str = field(default_factory=lambda: _env("WATSONX_APIKEY"))
    project_id: str = field(default_factory=lambda: _env("WATSONX_PROJECT_ID"))
    space_id: str = field(default_factory=lambda: _env("WATSONX_SPACE_ID"))
    # The API dates its contract; pin it so a server-side default cannot move.
    api_version: str = field(default_factory=lambda: _env("WATSONX_API_VERSION", "2023-10-25"))
    timeout_s: float = field(default_factory=lambda: float(_env("WATSONX_TIMEOUT_S", "60")))
    # Inputs per request. One call per chunk would be 1000 round trips per
    # student; this is what makes a class of thirty ~940 requests in total.
    max_batch: int = field(default_factory=lambda: _int("WATSONX_MAX_BATCH", 32))
    max_retries: int = field(default_factory=lambda: _int("WATSONX_MAX_RETRIES", 4))
    retry_base_s: float = field(default_factory=lambda: float(_env("WATSONX_RETRY_BASE_S", "1.0")))

    def require(self) -> None:
        """Fail at submit time with a readable message, not at the first chunk
        with a 401."""
        missing = []
        if not self.api_key:
            missing.append("WATSONX_APIKEY")
        if not self.project_id and not self.space_id:
            missing.append("WATSONX_PROJECT_ID (or WATSONX_SPACE_ID)")
        if missing:
            raise RuntimeError(
                "watsonx.ai embedding is enabled but " + " and ".join(missing) + " is not set; "
                "set it, or run with PIPELINE_EMBED=false"
            )
        if self.project_id and self.space_id:
            raise RuntimeError("set exactly one of WATSONX_PROJECT_ID and WATSONX_SPACE_ID, not both")


@dataclass(frozen=True)
class GenerationConfig:
    """The model that *answers*, which no Flink job ever calls.

    It belongs here anyway, beside the embedding model, because the two are
    chosen together and constrained the same way: the catalogue is regional.
    ``ca-tor`` — the workshop's region — serves exactly two chat models,
    ``meta-llama/llama-3-3-70b-instruct`` and
    ``mistralai/mistral-small-3-1-24b-instruct-2503``. Anything else is a 404
    on the first question, not a fallback. The list is
    ``GET /ml/v1/foundation_model_specs?filters=function_text_chat``.

    Read by the inspector's Ask tab (``dashboard/inspector/rag.py``), which
    retrieves from the same index the pipeline wrote and asks this model to
    answer out of what it retrieved.
    """

    model_id: str = field(
        default_factory=lambda: _env("WATSONX_LLM_MODEL_ID", "meta-llama/llama-3-3-70b-instruct")
    )
    max_tokens: int = field(default_factory=lambda: _int("WATSONX_LLM_MAX_TOKENS", 700))
    # Zero on purpose: the answer is supposed to be the retrieved text, and a
    # sampled one is harder to compare against the chunks shown beside it.
    temperature: float = field(default_factory=lambda: float(_env("WATSONX_LLM_TEMPERATURE", "0")))


@dataclass(frozen=True)
class OpenSearchConfig:
    """Where ``SINK_TYPE`` writes when it names ``opensearch``.

    The workshop cluster presents a certificate from a private CA, exactly as
    the Kafka brokers do, so ``OPENSEARCH_CA_LOCATION`` is a path to that CA in
    PEM form — and, like ``KAFKA_CA_LOCATION``, it must be a path *inside the
    container*, not on the laptop. The FlinkApplication mounts it from the
    workshop Secret; see ``flink/application-workshop.json.tmpl``.

    Turning verification off instead (``OPENSEARCH_VERIFY_CERTS=false``) works
    and is the escape hatch when the mount is not there, but it is worth not
    reaching for by default: the same private-CA story is one a student has
    already met at the Kafka end, and meeting it twice is the point.

    Every student owns one index and only their own: the cluster's security
    plugin grants ``studentNN`` full rights on ``studentNN-*`` and nothing
    else, so ``OPENSEARCH_INDEX`` has to start with the username. A name
    outside that prefix is a 403 on the first write, not on the create.
    """

    hosts: str = field(default_factory=lambda: _env("OPENSEARCH_HOSTS", "https://localhost:9200"))
    index: str = field(default_factory=lambda: _env("OPENSEARCH_INDEX", "document-chunks"))
    username: str = field(default_factory=lambda: _env("OPENSEARCH_USERNAME", "admin"))
    password: str = field(default_factory=lambda: _env("OPENSEARCH_PASSWORD"))
    use_ssl: bool = field(default_factory=lambda: _flag("OPENSEARCH_USE_SSL", True))
    verify_certs: bool = field(default_factory=lambda: _flag("OPENSEARCH_VERIFY_CERTS", True))
    ca_certs: str = field(default_factory=lambda: _env("OPENSEARCH_CA_LOCATION"))

    def host_list(self) -> list[str]:
        return [h.strip() for h in self.hosts.split(",") if h.strip()]

    @property
    def is_https(self) -> bool:
        return any(h.startswith("https://") for h in self.host_list())

    def client_kwargs(self) -> dict:
        kwargs: dict = {
            "hosts": self.host_list(),
            "use_ssl": self.use_ssl,
            "verify_certs": self.verify_certs,
            "ssl_show_warn": self.verify_certs,
        }
        if self.ca_certs:
            kwargs["ca_certs"] = self.ca_certs
        if self.username and self.password:
            kwargs["http_auth"] = (self.username, self.password)
        return kwargs

    def require(self) -> None:
        """Fail at submit time with a readable message.

        Every one of these surfaces, if left alone, as an SSL handshake or a
        403 on the first record — an hour into the class, in a TaskManager log,
        attributed to nothing in particular.
        """
        if not self.hosts:
            raise RuntimeError(
                "SINK_TYPE names opensearch but OPENSEARCH_HOSTS is empty; set it, "
                "or drop opensearch from SINK_TYPE"
            )
        if not self.index:
            raise RuntimeError("OPENSEARCH_INDEX is empty")
        if self.username and not self.password:
            raise RuntimeError(
                f"OPENSEARCH_USERNAME={self.username!r} but OPENSEARCH_PASSWORD is empty — "
                "in the cluster it comes from the workshop Secret, on a laptop from lab.yaml"
            )
        # A username that does not prefix the index is the 403 that looks like
        # a broken pipeline: the job starts, consumes, embeds, and every write
        # is refused.
        if self.username.startswith("student") and not self.index.startswith(f"{self.username}-"):
            raise RuntimeError(
                f"OPENSEARCH_INDEX={self.index!r} is outside {self.username}'s namespace — "
                f"the cluster only grants {self.username} rights on '{self.username}-*', so "
                f"every write would be refused. Use '{self.username}-chunks'."
            )
        if self.verify_certs and self.is_https and self.ca_certs and not os.path.isfile(self.ca_certs):
            raise RuntimeError(
                f"OPENSEARCH_CA_LOCATION={self.ca_certs!r} does not exist in this container. "
                "It is mounted from the workshop Secret (key 'ca.pem') — check that the Secret "
                "has it, or set OPENSEARCH_VERIFY_CERTS=false to skip verification."
            )
