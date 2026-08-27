"""The stage graph of each job, as the code actually builds it.

This is a description of ``src/pipeline/enrich_job.py`` and
``src/pipeline/full_job.py`` — same stage names, same order, same optional
stages — with two things attached to every node that the job graph does not
carry: the Kafka topic it reads or writes (so a message count can be put on
it), and one line on why the stage exists at all.

The optional stages are resolved from the *live* configuration, read through
``pipeline.config`` itself. A student who sets ``PIPELINE_EMBED=false`` sees
the embed stage greyed out here, because that is what the deployed job does.

The graph starts *before* Flink. The documents arrive by being uploaded to an
IBM Cloud Object Storage bucket, whose object-created event triggers a Docling
SaaS job, whose ``kafka_chunks`` target writes the chunk topic. None of that is Flink and none of it can be
probed from here, so those nodes are drawn as what they are — external services
with a link to their console — and the first thing the dashboard can actually
measure is the topic they feed. There is no second version of that path on the
diagram: the ingest scripts are a development shortcut around it, not a way
documents arrive.

``flink_name`` is the string the job passes to ``.name()``. It is how a node is
matched to a vertex in Flink's REST output, and several nodes legitimately match
the same one: Flink chains operators into a single vertex wherever the edge
between them is one-to-one, so a vertex is named for everything fused into it
(``Source: docling.chunks -> prepare``). A chain breaks at a
shuffle — every ``key_by`` — and wherever a job asks for a break explicitly.
``python.operator-chaining.enabled`` is *not* what draws the boxes: it decides
how many Python operators exist (and so how many Python worker processes), not
how they are grouped into vertices.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from labtools.config import docling_workbench_url
from pipeline.config import KafkaConfig, PipelineConfig, sink_types_from_env

TOPIC, OP, SINK, EXT = "topic", "op", "sink", "external"


def cmf_app_from_env(default: str) -> str:
    """The CMF application to ask about, which is per *student*, not per job.

    Everything else on the diagram is namespaced by the topic names, and those
    already come from the environment — so pointing the dashboard at a student
    used to move every Kafka panel and leave this one behind, asking CMF for
    ``docling-chunk-enrich`` and rendering NOT_DEPLOYED beside topics with data
    visibly flowing through them. That reads as "your job is dead" when the job
    is fine, which is the worst thing a teaching dashboard can say.

    ``./pipeline.sh inspect`` sets this along with the topics.
    """
    return os.environ.get("CMF_APPLICATION") or default


@dataclass(frozen=True)
class Node:
    id: str
    label: str
    kind: str = OP
    topic: str = ""          # the topic this node reads (kind=topic) or writes (kind=sink)
    flink_name: str = ""     # the .name() given to the operator in the job
    why: str = ""            # the Flink primitive the stage exists to use
    enabled: bool = True
    group: str = "main"      # main | control | side | ingest
    url: str = ""            # console/API this node stands for (kind=external)
    shape: str = ""          # graphviz shape override


@dataclass(frozen=True)
class Edge:
    src: str
    dst: str
    label: str = ""
    side: bool = False       # a side output / control path, drawn dashed


@dataclass(frozen=True)
class Topology:
    key: str                 # enrich | full
    title: str
    module: str
    flink_job_name: str
    cmf_app: str
    input_topic: str
    consumer_group: str
    nodes: list[Node]
    edges: list[Edge]
    sink_type: str
    parallelism: int = 1
    notes: list[str] = field(default_factory=list)

    @property
    def by_id(self) -> dict[str, Node]:
        return {n.id: n for n in self.nodes}

    def topics(self) -> list[str]:
        """Every topic this job reads or writes, input first, in graph order."""
        seen: list[str] = []
        for node in self.nodes:
            if node.topic and node.enabled and node.topic not in seen:
                seen.append(node.topic)
        return seen


# The path a document takes before Flink sees it, and it is the same path every
# time: a document is uploaded to a COS bucket, and the object-created event
# triggers a Docling job whose target writes the chunk topic. Two boxes, because
# two is what a student has to hold in their head — what wakes Docling is worth a
# sentence, not a node. Nobody converts anything on a laptop, either: the scripts
# in ``scripts/`` are a development shortcut around this path, not a second
# version of it.
#
# The nodes are drawn whether or not the links are configured; without
# ``COS_BUCKET_CRN`` they are simply greyed out, because the shape of the system
# does not depend on whether this dashboard knows the console URL.
def _ingest_chain(input_id: str = "in") -> tuple[list[Node], list[Edge]]:
    bucket_url = os.environ.get("COS_BUCKET_URL", "")
    bucket = os.environ.get("COS_BUCKET", "")
    # The link is the workbench where a student can watch the job, not the API
    # endpoint the job is submitted to — the same instance, under the hostname
    # a browser can open.
    service_url = os.environ.get("DOCLING_SERVICE_URL", "")
    docling_url = docling_workbench_url(service_url) or service_url
    live = bool(bucket_url)

    nodes = [
        Node("cos", f"1 · Upload here\nIBM COS · {bucket}" if bucket else "1 · Upload here\nIBM COS bucket",
             EXT, group="ingest", shape="folder", url=bucket_url, enabled=live,
             why="where a document enters the system — dropping a file in the bucket is the "
                 "only manual step, and everything after it is a consequence of that write"),
        Node("docling", "2 · Docling SaaS\nconvert + chunk", EXT, group="ingest", shape="box3d",
             url=docling_url, enabled=live,
             why="the object-created event triggers a Docling job: it converts the document "
                 "and chunks it, and its kafka_chunks target produces one message per chunk — "
                 "conversion and chunking never happen in Flink"),
    ]
    edges = [
        Edge("cos", "docling", "object-created event", side=not live),
        Edge("docling", input_id, "kafka_chunks", side=not live),
    ]
    return nodes, edges


def _spine_edges(order: list[str], enabled: set[str]) -> list[Edge]:
    """Edges along a chain of stages, some of which are switched off.

    A disabled stage stays on the diagram — a student should see that the
    pipeline *has* a quality filter and that it is off — so it is drawn greyed
    out with dashed edges, and a solid edge bypasses it to show where the
    records really go.
    """
    dashed = {(a, b) for a, b in zip(order, order[1:])}
    live = [n for n in order if n in enabled]
    solid = [(a, b) for a, b in zip(live, live[1:])]
    edges = [Edge(a, b) for a, b in solid]
    edges += [Edge(a, b, side=True) for a, b in dashed if (a, b) not in set(solid)]
    return edges


def _extra_sink_edges(spine: list[str], enabled: set[str], sinks: list[Node]) -> list[Edge]:
    """Edges to the second and later terminals, from wherever the first one is fed.

    The fork has to hang off the same node the main sink edge leaves, and that
    is the last *enabled* stage — not simply ``embed``, which a student running
    with ``PIPELINE_EMBED=false`` does not have.
    """
    live = [n for n in spine if n in enabled]
    if not live or len(sinks) < 2:
        return []
    return [Edge(live[-1], s.id) for s in sinks[1:]]


def _sink_node(sink_type: str, node_id: str, output_topic: str, index: str = "") -> Node:
    if sink_type == "kafka":
        return Node(node_id, output_topic, SINK, topic=output_topic, flink_name="kafka-sink",
                    why="the finished record on a topic — scripts/drain_topic.py reads it")
    if sink_type == "opensearch":
        return Node(node_id, f"OpenSearch\n{index}" if index else "OpenSearch", SINK,
                    flink_name="opensearch",
                    why="indexed with its vector — this is what the Ask tab retrieves from")
    if sink_type == "log":
        return Node(node_id, "TaskManager log", SINK, flink_name="log",
                    why="one summary line per chunk")
    return Node(node_id, "discard", SINK, flink_name="none", why="counted and dropped")


def _sink_nodes(sink_types: tuple[str, ...], output_topic: str) -> list[Node]:
    """One node per terminal ``SINK_TYPE`` names, all fed by the same stage.

    The workshop's default names two — the output topic *and* the index — so
    the diagram forks at the end. That fork is real and costs nothing: both
    terminals are attached to the same already-partitioned stream, so no record
    moves between subtasks to reach either.

    The first keeps the id ``sink``, because that is the one the page reads the
    output topic off; the rest are ``sink:<type>``.
    """
    index = os.environ.get("OPENSEARCH_INDEX", "")
    return [
        _sink_node(t, "sink" if i == 0 else f"sink:{t}", output_topic, index)
        for i, t in enumerate(sink_types)
    ]


def enrich_topology() -> Topology:
    """``pipeline.enrich_job`` — the workshop's step 3, the spine on its own.

    Stateless, deliberately: no keyed state, no TTL, and one shuffle in the
    whole graph. Dedup is not missing from this drawing — it is not in the job.
    It arrives in step 4 along with the guard, which is where keyed state is
    worth a demonstration of its own.
    """
    kafka, cfg, sink_types = KafkaConfig(), PipelineConfig(), sink_types_from_env()
    sinks = _sink_nodes(sink_types, kafka.output_topic)
    nodes = [
        Node("in", kafka.chunk_topic, TOPIC, topic=kafka.chunk_topic,
             why="what Docling's kafka_chunks target produces — one message per chunk"),
        Node("prepare", "prepare", flink_name="prepare",
             why="drop empties, normalize the text, then enrich — three stages in one "
                 "operator, and the fingerprint is taken after the normalization, "
                 "which is what the full pipeline's dedup stage later keys on"),
        Node("embed", "embed", flink_name="embed", enabled=cfg.embed,
             why=f"watsonx.ai, micro-batched ({cfg.embed_batch_size} / "
                 f"{cfg.embed_batch_delay_ms}ms) and bounded by a timer"),
        *sinks,
    ]
    enabled = {n.id for n in nodes if n.enabled}
    edges = _spine_edges(["in", "prepare", "embed", sinks[0].id], enabled)
    edges += _extra_sink_edges(["in", "prepare", "embed"], enabled, sinks)
    ingest_nodes, ingest_edges = _ingest_chain()
    nodes = ingest_nodes + nodes
    edges = ingest_edges + edges
    return Topology(
        key="enrich",
        title="chunk-enrich pipeline",
        module="pipeline.enrich_job",
        flink_job_name="chunk-enrich-pipeline",
        cmf_app=cmf_app_from_env("docling-chunk-enrich"),
        input_topic=kafka.chunk_topic,
        consumer_group=kafka.consumer_group,
        nodes=nodes,
        edges=edges,
        sink_type="+".join(sink_types),
        notes=[
            "Operator chaining is on in this job, so Flink fuses most stages into "
            "one vertex — the per-stage busy percentages below are the vertex's.",
            "This pipeline holds no state. Deduplication is part of the full "
            "pipeline in step 4, not of this one.",
        ],
    )


def full_topology() -> Topology:
    """``pipeline.full_job`` — the workshop's step 4.

    Five stages, and the two that a student can point at are the ones this job
    exists for: a second *source* carrying the policy, and a guard fed by it
    that forks three ways. The stage names are the ones the job passes to
    ``.name()``, and the vertex they belong to is named for everything Flink
    fused into it — ``prepare`` shares the source's vertex, and the two audit
    sinks share the guard's.
    """
    kafka, cfg = KafkaConfig(), PipelineConfig()
    sink_types = sink_types_from_env()
    sinks = _sink_nodes(sink_types, kafka.output_topic)

    nodes: list[Node] = [
        Node("in", kafka.chunk_topic, TOPIC, topic=kafka.chunk_topic,
             why="what Docling's kafka_chunks target produces — one message per chunk"),
        Node("policy", kafka.policy_topic, TOPIC, topic=kafka.policy_topic, group="control",
             why="the control plane: one message here changes what the *running* job "
                 "redacts and drops — no restart, no redeploy"),
        Node("prepare", "prepare", flink_name="prepare",
             why="drop empties, normalize the text, then enrich — three stages in one "
                 "operator, because this job runs without the Python chaining optimizer"),
        Node("pii-guard", "pii-guard", flink_name="pii-guard",
             why="applies the broadcast policy: redacts PII in the main stream and "
                 "sends the original to the audit topic; the quality gate drops here too"),
        Node("dedup", "dedup", flink_name="dedup", enabled=cfg.dedup,
             why="new in this pipeline: keyed + TTL'd state on the fingerprint — the "
                 "stage that genuinely needs Flink, and the one that catches the same "
                 "content arriving twice under two different file names"),
        Node("embed", "embed", flink_name="embed", enabled=cfg.embed,
             why=f"watsonx.ai, micro-batched ({cfg.embed_batch_size} / "
                 f"{cfg.embed_batch_delay_ms}ms) and bounded by a timer"),
        *sinks,
        # --- side-output topics ------------------------------------------
        Node("t-quarantine", kafka.quarantine_topic, SINK, topic=kafka.quarantine_topic, group="side",
             why="the untouched original of every chunk that contained PII"),
        Node("t-rejected", kafka.rejected_topic, SINK, topic=kafka.rejected_topic, group="side",
             why="what the quality gate dropped, kept visible instead of discarded"),
    ]
    enabled = {n.id for n in nodes if n.enabled}
    spine = ["in", "prepare", "pii-guard", "dedup", "embed"]
    edges = _spine_edges([*spine, sinks[0].id], enabled)
    edges += _extra_sink_edges(spine, enabled, sinks)

    ingest_nodes, ingest_edges = _ingest_chain()
    nodes = ingest_nodes + nodes
    edges += ingest_edges
    edges += [
        Edge("policy", "pii-guard", "broadcast", side=True),
        Edge("pii-guard", "t-quarantine", "PII original", side=True),
        Edge("pii-guard", "t-rejected", "dropped", side=True),
    ]

    return Topology(
        key="full",
        title="full pipeline",
        module="pipeline.full_job",
        flink_job_name="chunk-guard-pipeline",
        cmf_app=cmf_app_from_env("docling-chunk-full"),
        input_topic=kafka.chunk_topic,
        consumer_group=kafka.consumer_group,
        nodes=nodes,
        edges=edges,
        sink_type="+".join(sink_types),
        notes=[
            "The guard is a broadcast operator, so this job runs with the Python "
            "chaining optimizer off — which costs Python worker processes but does "
            "not change the shape here: Flink still draws five vertices.",
            "Nothing is redacted or dropped until a rule is published to "
            f"`{kafka.policy_topic}` — the job starts from its defaults.",
        ],
    )


def load(key: str) -> Topology:
    return {"enrich": enrich_topology, "full": full_topology}[key]()
