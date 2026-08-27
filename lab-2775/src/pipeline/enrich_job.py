"""The workshop's first pipeline: the spine, and nothing else.

Conversion and chunking happen in Docling, not here: a document uploaded to the
bucket triggers a Docling job whose ``kafka_chunks`` target produces one message
per chunk. Flink starts from that topic.

    Kafka (chunks)
        -> prepare   drop empties, normalize the text, add the derived fields
        -> embed     watsonx.ai, in timer-bounded batches
        -> sink      kafka | opensearch | log | none

Stateless on purpose, and that is what makes it the one to read first: every
stage is a function of a single record, there is no keyed state to reason about
and no TTL to explain, and the graph contains exactly one shuffle — embed's, and
only so each subtask gets its own batch buffer.

``pipeline.full_job`` is this list with the stateful and control-plane parts
added: the dedup stage, and the PII guard its broadcast policy drives.

Submit it as a module::

    flink run -pym pipeline.enrich_job
"""

from __future__ import annotations

import logging

from pyflink.datastream import StreamExecutionEnvironment

from . import graph
from .config import (
    EmbeddingConfig,
    KafkaConfig,
    OpenSearchConfig,
    PipelineConfig,
    WatsonxConfig,
    sink_types_from_env,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

JOB_NAME = "chunk-enrich-pipeline"


def build_pipeline(
    env: StreamExecutionEnvironment,
    kafka: KafkaConfig,
    cfg: PipelineConfig,
    embedding: EmbeddingConfig,
    opensearch: OpenSearchConfig,
    watsonx: WatsonxConfig,
    sink_types: tuple[str, ...],
) -> None:
    stream = graph.source(env, kafka, kafka.chunk_topic, kafka.consumer_group)
    stream = graph.prepare(stream, cfg)
    stream = graph.embed(stream, cfg, embedding, watsonx)
    graph.sink(stream, sink_types=sink_types, kafka=kafka, opensearch=opensearch, topic=kafka.output_topic)


def main() -> None:
    kafka = KafkaConfig()
    cfg = PipelineConfig()
    embedding = EmbeddingConfig()
    opensearch = OpenSearchConfig()
    watsonx = WatsonxConfig()
    sink_types = sink_types_from_env()

    # Fail at submit time, not at the first chunk an hour into the class.
    if cfg.embed:
        watsonx.require()
    if "opensearch" in sink_types:
        opensearch.require()

    logger.info(
        "%s: %s -> %s (sink=%s)",
        JOB_NAME, kafka.chunk_topic, kafka.output_topic, "+".join(sink_types),
    )

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(cfg.parallelism)
    build_pipeline(env, kafka, cfg, embedding, opensearch, watsonx, sink_types)
    env.execute(JOB_NAME)


if __name__ == "__main__":
    main()
