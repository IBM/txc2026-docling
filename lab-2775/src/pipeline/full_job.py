"""The workshop's second pipeline: the first one plus a control plane.

Step 3 of the class deploys :mod:`pipeline.enrich_job`; step 4 deploys this. It
is the same list of stages with two more in the middle — the guard, and the
dedup stage the simple job deliberately does without:

    Kafka (chunks)                 Kafka (policy)
        |                              |  broadcast
        v                              v
      prepare  ------------------>  pii-guard  --+--> quarantine  (PII originals)
                                         |       +--> rejected    (dropped)
                                         v
                                       dedup
                                         |
                                         v
                                       embed
                                         |
                                         v
                                       sink

The guard is the point. Its rules arrive on a broadcast topic, so publishing one
message changes what a *running* job redacts and drops — no restart, no rebuild,
no redeploy. It is the only stage whose behaviour a student can change from a
keyboard while watching the output move.

Dedup arrives here too, and for the same teaching reason as the guard: it is the
stage that genuinely needs Flink rather than a script — keyed, checkpointed,
TTL'd state, against an at-least-once producer — and it is worth a demonstration
of its own rather than being present, unexplained, from step 3.

What it costs, and why the graph is shaped like this
----------------------------------------------------
The guard is a ``BroadcastProcessFunction``, and PyFlink's chaining optimizer
cannot rewrite a broadcast-connected operator (``NoSuchFieldException:
regularInput``). So this job runs with ``python.operator-chaining.enabled=false``
and *every* Python operator becomes its own Python worker process. That is why
:mod:`pipeline.stages.prepare` and :mod:`pipeline.stages.dedup` each fuse
several stages by hand: the optimizer is not there to do it.

It costs nothing in the picture. The boxes in Flink's UI are job vertices, which
come from Flink's own operator chaining and not from that flag, so this graph
still draws as five: the two sources, the guard, dedup and embed.

Submit it as a module::

    flink run -pym pipeline.full_job
"""

from __future__ import annotations

import logging

from pyflink.common import Configuration, Types
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
from .stages.guard import POLICY_DESCRIPTOR, QUARANTINE_TAG, REJECTED_TAG, PolicyGuardFunction
from .logic.policy import Policy

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

JOB_NAME = "chunk-guard-pipeline"


def build_pipeline(
    env: StreamExecutionEnvironment,
    kafka: KafkaConfig,
    cfg: PipelineConfig,
    embedding: EmbeddingConfig,
    opensearch: OpenSearchConfig,
    watsonx: WatsonxConfig,
    sink_types: tuple[str, ...],
) -> None:
    chunks = graph.source(env, kafka, kafka.chunk_topic, kafka.consumer_group)
    # The control plane. Low volume, and broadcast rather than keyed: every
    # subtask needs every rule, because any record may be the one it applies to.
    policy = graph.source(env, kafka, kafka.policy_topic, f"{kafka.consumer_group}-policy").name("policy-rules")

    # The quality gate belongs to the guard in this job, so prepare only flags.
    prepared = graph.prepare(chunks, cfg, drop_low_quality=False)

    # The defaults are the job's starting position, not the last word: the first
    # message on the policy topic replaces them, on the next record.
    defaults = Policy(min_chars=cfg.min_chars, drop_low_quality=cfg.drop_low_quality)
    guarded = (
        prepared.connect(policy.broadcast(POLICY_DESCRIPTOR))
        .process(PolicyGuardFunction(defaults), output_type=Types.STRING())
        .name("pii-guard")
    )
    # Two audit trails, and they are the visible half of the stage: what was
    # redacted, and what was dropped. A quality gate whose drops go nowhere is
    # indistinguishable from a broken pipeline.
    graph.to_topic(guarded.get_side_output(QUARANTINE_TAG), kafka, kafka.quarantine_topic, "pii-quarantine")
    graph.to_topic(guarded.get_side_output(REJECTED_TAG), kafka, kafka.rejected_topic, "quality-rejected")

    stream = graph.dedup(guarded, cfg)
    stream = graph.embed(stream, cfg, embedding, watsonx)
    graph.sink(stream, sink_types=sink_types, kafka=kafka, opensearch=opensearch, topic=kafka.output_topic)


def job_configuration() -> Configuration:
    """``python.operator-chaining.enabled=false`` — a workaround, not tuning.

    PyFlink's ``PythonOperatorChainingOptimizer`` cannot rewrite the input of a
    broadcast-connected operator and throws ``NoSuchFieldException:
    regularInput`` while building the graph, so any job that feeds a Python
    operator into ``.connect(broadcast)`` fails at *submit* time with the
    optimizer on. There is no per-operator escape: the flag is global.
    """
    conf = Configuration()
    conf.set_string("python.operator-chaining.enabled", "false")
    return conf


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
        "%s: %s (+ policy %s) -> %s (sink=%s)",
        JOB_NAME, kafka.chunk_topic, kafka.policy_topic, kafka.output_topic, "+".join(sink_types),
    )

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(cfg.parallelism)
    env.configure(job_configuration())
    build_pipeline(env, kafka, cfg, embedding, opensearch, watsonx, sink_types)
    env.execute(JOB_NAME)


if __name__ == "__main__":
    main()
