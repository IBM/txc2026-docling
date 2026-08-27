"""Kafka source and sink builders shared by every job in the repo.

One place where the environment-driven :class:`~pipeline.config.KafkaConfig` is
turned into Flink connector properties, so the local PLAINTEXT broker, the
hosted SASL_SSL brokers and the in-cluster listener a CMF deployment talks to
differ only by environment variables.
"""

from __future__ import annotations

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

from .config import KafkaConfig

# Properties the builders set themselves; passing them again would be ignored
# at best and conflicting at worst.
_BUILDER_OWNED = frozenset({"bootstrap.servers", "group.id"})


def _apply_properties(builder, kafka: KafkaConfig):
    for key, value in kafka.properties().items():
        if key in _BUILDER_OWNED:
            continue
        builder = builder.set_property(key, value)
    return builder


def build_kafka_source(kafka: KafkaConfig, *, topic: str, group_id: str) -> KafkaSource:
    """Source for ``topic``, always from the earliest offset.

    Earliest so a (re)start picks up chunks already sitting on the topic: the
    ingest is a batch that finishes long before the job is submitted, and a
    student who deploys the other pipeline expects to see everything they have
    uploaded so far go through it again.
    """
    builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka.bootstrap_servers)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
    )
    return _apply_properties(builder, kafka).build()


def build_kafka_sink(kafka: KafkaConfig, topic: str) -> KafkaSink:
    """At-least-once string sink — the output topic and every side-output topic."""
    builder = (
        KafkaSink.builder()
        .set_bootstrap_servers(kafka.bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    )
    return _apply_properties(builder, kafka).build()
