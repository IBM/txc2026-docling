#!/usr/bin/env python3
"""Laptop-side Kafka helper: create the pipeline's topics, list, produce, tail.

Uses the same connection logic as every other script here
(:mod:`labtools.kafka`), so it works unchanged against the local PLAINTEXT broker
and against the hosted SASL_SSL listener:

    ./setup.sh topics                     # what a student actually runs

    python scripts/kafka_tool.py setup                  # create every topic the jobs need
    python scripts/kafka_tool.py list
    python scripts/kafka_tool.py create docling.chunks --partitions 3
    python scripts/kafka_tool.py delete docling.chunks.enriched
    python scripts/kafka_tool.py produce policy-rules '{"pii_redact": true}'
    python scripts/kafka_tool.py tail docling.chunks -n 5

The brokers here have auto-creation disabled, exactly as Confluent Cloud does,
so ``setup`` is a required step and not a convenience.
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from labtools.kafka import client_config, ensure_topic, require_confluent_kafka

# The five topics the two jobs use, in the order the graph touches them. Each
# is a real input or output: a missing one fails the job at runtime, and the
# brokers do not auto-create.
TOPICS = [
    os.environ.get("KAFKA_CHUNKS_TOPIC", "docling.chunks"),
    os.environ.get("KAFKA_OUTPUT_TOPIC", "docling.chunks.enriched"),
    os.environ.get("KAFKA_POLICY_TOPIC", "policy-rules"),
    os.environ.get("KAFKA_QUARANTINE_TOPIC", "pii-quarantine"),
    os.environ.get("KAFKA_REJECTED_TOPIC", "quality-rejected"),
]


def setup(args) -> None:
    conf = client_config(args.bootstrap)
    for topic in TOPICS:
        ensure_topic(conf, topic, partitions=args.partitions)
    print(f"{len(TOPICS)} topic(s) present")


def create(args) -> None:
    ensure_topic(client_config(args.bootstrap), args.topic, partitions=args.partitions)


def delete(args) -> None:
    from confluent_kafka.admin import AdminClient

    admin = AdminClient(client_config(args.bootstrap))
    for name, fut in admin.delete_topics([args.topic]).items():
        fut.result()
        print(f"deleted topic {name!r}")


def list_topics(args) -> None:
    require_confluent_kafka()
    from confluent_kafka.admin import AdminClient

    md = AdminClient(client_config(args.bootstrap)).list_topics(timeout=15)
    for name in sorted(md.topics):
        if name.startswith("_") and not args.all:
            continue
        print(f"{name:40s} partitions={len(md.topics[name].partitions)}")


def produce(args) -> None:
    from confluent_kafka import Producer

    p = Producer(client_config(args.bootstrap))
    p.produce(args.topic, value=args.value.encode(), key=args.key.encode() if args.key else None)
    p.flush(10)
    print(f"produced 1 message -> {args.topic}")


def tail(args) -> None:
    from confluent_kafka import Consumer, TopicPartition

    conf = client_config(args.bootstrap)
    conf.update({"group.id": "kafka-tool-tail", "enable.auto.commit": False, "auto.offset.reset": "latest"})
    c = Consumer(conf)
    partitions = c.list_topics(args.topic, timeout=10).topics[args.topic].partitions
    assignment = []
    for pid in partitions:
        _, high = c.get_watermark_offsets(TopicPartition(args.topic, pid), timeout=10)
        assignment.append(TopicPartition(args.topic, pid, max(0, high - args.n)))
    c.assign(assignment)
    seen = 0
    while seen < args.n:
        msg = c.poll(5)
        if msg is None:
            break
        if msg.error():
            continue
        print(msg.value().decode(errors="replace"))
        seen += 1
    c.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", help="Kafka bootstrap servers (default: env or localhost:29092)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("setup", help="create every topic the pipelines need")
    s.add_argument("--partitions", type=int, default=3)
    s.set_defaults(fn=setup)

    c = sub.add_parser("create"); c.add_argument("topic"); c.add_argument("--partitions", type=int, default=3)
    c.set_defaults(fn=create)

    d = sub.add_parser("delete"); d.add_argument("topic"); d.set_defaults(fn=delete)

    ls = sub.add_parser("list"); ls.add_argument("--all", action="store_true", help="include internal topics")
    ls.set_defaults(fn=list_topics)

    pr = sub.add_parser("produce"); pr.add_argument("topic"); pr.add_argument("value"); pr.add_argument("--key")
    pr.set_defaults(fn=produce)

    tl = sub.add_parser("tail"); tl.add_argument("topic"); tl.add_argument("-n", type=int, default=5)
    tl.set_defaults(fn=tail)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
