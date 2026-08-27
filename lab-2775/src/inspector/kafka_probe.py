"""What Kafka can tell us about the pipeline without touching it.

Three questions, three functions:

* ``topic_stats``  — how many messages are on each topic (the high watermark
  minus the low one, summed over partitions).
* ``group_lag``    — how far behind a job's consumer group is. Non-zero lag on
  the input topic is the honest answer to "is this stage actually working?".
* ``read_topic``   — the last N messages, for looking at them.

Everything reads: no consumer group is joined and no offset is ever committed,
so the dashboard cannot disturb a running job no matter how often a student
hits refresh. That is the same discipline ``scripts/drain_topic.py`` follows,
and this module borrows its connection setup (``labtools.kafka``) so
the two cannot drift apart.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient

from labtools.kafka import client_config

# Watermark and offset lookups are single round trips to a broker that is
# either on this laptop or one hop away; ten seconds is a hang, not a wait.
TIMEOUT_S = 6.0


@dataclass
class TopicStats:
    topic: str
    exists: bool = True
    partitions: int = 0
    total: int = 0                      # messages currently retained
    end_offset: int = 0                 # sum of high watermarks
    per_partition: dict[int, tuple[int, int]] = field(default_factory=dict)
    error: str = ""


@dataclass
class GroupLag:
    """How far a consumer group has got — *if* it says so.

    A Flink Kafka source only commits offsets back to the group on a completed
    checkpoint. None of the jobs here enable checkpointing, so the group stays
    empty while the job happily consumes, and a naive "lag = end - committed"
    reports the whole topic as a backlog forever. ``tracked`` is that
    distinction: false means "this group publishes no progress", which is not
    the same as "this group is stuck".
    """

    group: str
    topic: str
    lag: int = 0
    committed: int = 0
    end_offset: int = 0
    tracked: bool = False               # has the group committed anything at all?
    error: str = ""

    @property
    def backlog(self) -> int | None:
        """Messages waiting, or ``None`` when the group commits no offsets."""
        return self.lag if self.tracked else None


def _conf(extra: dict | None = None) -> dict:
    conf = client_config()
    conf.setdefault("socket.timeout.ms", 8000)
    if extra:
        conf.update(extra)
    return conf


def make_admin() -> AdminClient:
    return AdminClient(_conf())


def make_reader() -> Consumer:
    """A consumer that only ever reads: no group, no commits, no subscribe."""
    return Consumer(
        _conf(
            {
                "group.id": "pipeline-inspector",
                "enable.auto.commit": False,
                "auto.offset.reset": "error",
            }
        )
    )


def make_group_probe(group: str) -> Consumer:
    """A consumer used solely to *ask* for ``group``'s committed offsets.

    It never subscribes, so it never joins the group and never triggers a
    rebalance of the running job's consumers.
    """
    return Consumer(_conf({"group.id": group, "enable.auto.commit": False}))


def cluster_topics(admin: AdminClient) -> dict[str, int]:
    """Every topic on the cluster mapped to its partition count."""
    md = admin.list_topics(timeout=TIMEOUT_S)
    return {
        name: len(t.partitions)
        for name, t in md.topics.items()
        if not name.startswith("_")
    }


def topic_stats(reader: Consumer, topic: str) -> TopicStats:
    try:
        md = reader.list_topics(topic, timeout=TIMEOUT_S).topics[topic]
    except KafkaException as exc:
        return TopicStats(topic=topic, exists=False, error=str(exc))
    if md.error is not None:
        # The usual case: the topic has not been created yet (`make topics`).
        return TopicStats(topic=topic, exists=False, error=str(md.error))

    stats = TopicStats(topic=topic, partitions=len(md.partitions))
    for pid in md.partitions:
        try:
            low, high = reader.get_watermark_offsets(
                TopicPartition(topic, pid), timeout=TIMEOUT_S
            )
        except KafkaException as exc:
            stats.error = str(exc)
            continue
        stats.per_partition[pid] = (low, high)
        stats.total += max(0, high - low)
        stats.end_offset += high
    return stats


def group_lag(probe: Consumer, group: str, topic: str, stats: TopicStats) -> GroupLag:
    """Lag of ``group`` on ``topic``, given watermarks already fetched."""
    result = GroupLag(group=group, topic=topic, end_offset=stats.end_offset)
    if not stats.exists or not stats.per_partition:
        return result
    partitions = [TopicPartition(topic, pid) for pid in stats.per_partition]
    try:
        committed = probe.committed(partitions, timeout=TIMEOUT_S)
    except KafkaException as exc:
        result.error = str(exc)
        return result

    for tp in committed:
        low, high = stats.per_partition.get(tp.partition, (0, 0))
        if tp.offset is None or tp.offset < 0:
            # No commit for this partition yet: everything retained is backlog.
            result.lag += max(0, high - low)
            continue
        result.tracked = True
        result.committed += tp.offset
        result.lag += max(0, high - tp.offset)
    return result


def read_topic(
    reader: Consumer,
    topic: str,
    limit: int = 25,
    newest_first: bool = True,
    poll_timeout: float = 1.0,
) -> list[dict]:
    """The last ``limit`` messages on ``topic`` (or the first, oldest-first).

    Reads by explicit assignment from a computed offset, so it terminates:
    each partition is given its share of ``limit`` and the loop stops once the
    high watermarks captured at entry are reached.
    """
    stats = topic_stats(reader, topic)
    if not stats.exists or stats.total == 0:
        return []

    # Share the budget over the partitions that actually hold something: a
    # topic keyed by doc_id leaves partitions empty, and counting those in
    # would quietly return a fraction of what was asked for.
    live = {pid: (low, high) for pid, (low, high) in stats.per_partition.items() if high > low}
    per_partition = max(1, -(-limit // max(1, len(live))))
    assignment, wanted = [], 0
    for pid, (low, high) in live.items():
        start = max(low, high - per_partition) if newest_first else low
        # Bounded from both ends: reading "oldest first" on a topic with a
        # million records must still be a couple of round trips, not a drain.
        take = min(per_partition, high - start)
        if take <= 0:
            continue
        assignment.append(TopicPartition(topic, pid, start))
        wanted += take

    if not assignment:
        return []
    reader.assign(assignment)
    try:
        records, deadline = [], time.time() + max(4.0, poll_timeout * 4)
        while len(records) < wanted and time.time() < deadline:
            msg = reader.poll(poll_timeout)
            if msg is None:
                continue        # a slow fetch, not the end — the deadline ends this
            if msg.error():
                continue
            records.append(
                {
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "timestamp": msg.timestamp()[1],
                    "key": msg.key().decode(errors="replace") if msg.key() else None,
                    "value": msg.value().decode(errors="replace") if msg.value() else "",
                }
            )
    finally:
        reader.unassign()

    records.sort(key=lambda r: (r["timestamp"], r["partition"], r["offset"]), reverse=newest_first)
    return records[:limit]
