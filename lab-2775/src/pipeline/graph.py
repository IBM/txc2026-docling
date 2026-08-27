"""The graph: one function per box in the picture, wiring a stage into a job.

:mod:`pipeline.stages` holds the operators; this is where they are attached to a
stream. Both jobs are built from these calls, which is what keeps them honestly
comparable — ``pipeline.enrich_job`` is source → prepare → embed → sink, and
``pipeline.full_job`` is that list with two stages added: the PII guard spliced
in after prepare, and dedup after it. Anything that differs between the two is
visible in the job module rather than hidden behind a flag in here.

Each function takes a stream and returns one, and the optional stages return
their input untouched when the configuration switches them off — so a job never
branches on ``if cfg.dedup``.
"""

from __future__ import annotations

from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment

from .config import EmbeddingConfig, KafkaConfig, OpenSearchConfig, PipelineConfig, WatsonxConfig
from .kafka_io import build_kafka_sink, build_kafka_source
from .logic.keys import Shard, by_fingerprint
from .stages.dedup import DeduplicateFunction
from .stages.embed import EmbedFunction
from .stages.prepare import PrepareFunction
from .stages.sink import DiscardSinkFunction, LogSinkFunction


def source(env: StreamExecutionEnvironment, kafka: KafkaConfig, topic: str, group_id: str):
    """A Kafka topic as a stream of JSON strings, from the earliest offset."""
    return env.from_source(
        build_kafka_source(kafka, topic=topic, group_id=group_id),
        WatermarkStrategy.no_watermarks(),
        topic,
        type_info=Types.STRING(),
    )


def prepare(stream, cfg: PipelineConfig, *, drop_low_quality: bool | None = None):
    """Drop empties, normalize, enrich — see :mod:`pipeline.stages.prepare`.

    ``drop_low_quality`` is a parameter rather than only a config read because
    the full job hands its quality gate to the guard, where the threshold is a
    live policy instead of a start-up value.
    """
    quality = cfg.drop_low_quality if drop_low_quality is None else drop_low_quality
    return stream.flat_map(
        PrepareFunction(cfg.min_chars, drop_low_quality=quality), output_type=Types.STRING()
    ).name("prepare")


def dedup(stream, cfg: PipelineConfig):
    """Keyed, TTL'd state on the text fingerprint. The stage that needs Flink.

    Only ``pipeline.full_job`` wires this in. The simple job is deliberately
    stateless, so keyed state is something a student meets once, in step 4,
    rather than something already there before it has been explained.
    """
    if not cfg.dedup:
        return stream
    return (
        stream.key_by(by_fingerprint, key_type=Types.STRING())
        .process(DeduplicateFunction(cfg.dedup_ttl_hours), output_type=Types.STRING())
        .name("dedup")
    )


def embed(stream, cfg: PipelineConfig, embedding: EmbeddingConfig, watsonx: WatsonxConfig):
    """watsonx.ai, in timer-bounded batches, one buffer per shard key."""
    if not cfg.embed:
        return stream
    return (
        stream.key_by(Shard(cfg.embed_shards), key_type=Types.STRING())
        .process(
            EmbedFunction(
                embedding,
                batch_size=cfg.embed_batch_size,
                max_delay_ms=cfg.embed_batch_delay_ms,
                watsonx=watsonx,
            ),
            output_type=Types.STRING(),
        )
        .name("embed")
    )


def to_topic(stream, kafka: KafkaConfig, topic: str, name: str) -> None:
    """Write a stream — the output, or a side output — to its own topic."""
    stream.sink_to(build_kafka_sink(kafka, topic)).name(name)


def sink(stream, *, sink_types: tuple[str, ...], kafka: KafkaConfig,
         opensearch: OpenSearchConfig, topic: str) -> None:
    """Attach every terminal stage ``SINK_TYPE`` names to the same stream.

    ``SINK_TYPE`` is a list because the workshop wants both endings at once:
    ``kafka,opensearch`` writes the finished record to the output topic *and*
    indexes it with its vector. The topic is what makes enrichment legible one
    record at a time (``scripts/drain_topic.py``); the index is what the
    inspector's Ask tab retrieves from. Neither is a debugging fallback for the
    other, so a student should not have to pick.

    Fanning out costs no shuffle: each terminal is attached to the same
    already-partitioned stream, so Flink hands each record to both without
    moving it. What it does cost is one Python operator per non-Kafka terminal,
    which is why ``none`` refuses to be combined with anything (see
    :func:`pipeline.config.sink_types_from_env`) and why the default stays at
    two.

    OpenSearch is not always reachable — during bring-up it usually is not —
    and a pipeline that cannot run without its index is a pipeline you cannot
    debug. That is what ``SINK_TYPE=kafka`` on its own is still for.
    """
    for sink_type in sink_types:
        if sink_type == "kafka":
            to_topic(stream, kafka, topic, f"kafka-sink[{topic}]")
        elif sink_type == "opensearch":
            from .stages.sink import OpenSearchSinkFunction

            stream.map(OpenSearchSinkFunction(opensearch), output_type=Types.STRING()).name("opensearch-sink")
        elif sink_type == "log":
            stream.map(LogSinkFunction(), output_type=Types.STRING()).name("log-sink")
        else:  # "none"
            stream.map(DiscardSinkFunction(), output_type=Types.STRING()).name("discard-sink")
