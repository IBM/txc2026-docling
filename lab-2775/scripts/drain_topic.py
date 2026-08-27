#!/usr/bin/env python3
"""Read a topic to the end and print what is on it, then exit.

Reads every partition from its earliest offset up to the high watermark that
was current when the script started ("exhaust the queue"), prints each message
and stops — it does not join a consumer group and commits nothing, so it can be
re-run as often as you like without disturbing the Flink job's offsets.

    uv run scripts/drain_topic.py                          # the chunk topic
    uv run scripts/drain_topic.py --topic docling.chunks.enriched
    uv run scripts/drain_topic.py --topic docling.chunks --full
    uv run scripts/drain_topic.py --topic pii-quarantine --from latest --follow

The record layout is the same at every stage — the chunk record Docling wrote,
with the pipeline's derived fields added — so the same summary line works on the
input topic, the output topic and the side-output topics.
"""

from __future__ import annotations

import argparse
import os
import sys

from labtools.kafka import client_config, require_confluent_kafka
from labtools.records import render

DEFAULT_TOPIC = os.environ.get("KAFKA_CHUNKS_TOPIC", "docling.chunks")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--topic", default=DEFAULT_TOPIC, help=f"topic to read (default: {DEFAULT_TOPIC})")
    ap.add_argument("--bootstrap", help="Kafka bootstrap servers (default: env or localhost:29092)")
    ap.add_argument("--from", dest="start", choices=("earliest", "latest"), default="earliest")
    ap.add_argument("--max", type=int, help="stop after N messages")
    ap.add_argument("--timeout", type=float, default=10.0, help="seconds to wait for a message before giving up")
    ap.add_argument("--follow", action="store_true", help="keep waiting for new messages instead of exiting")
    ap.add_argument("--full", action="store_true", help="print the whole record instead of a one-line summary")
    ap.add_argument("--count-only", action="store_true", help="only report how many messages are on the topic")
    args = ap.parse_args()

    require_confluent_kafka()
    from confluent_kafka import Consumer, TopicPartition

    conf = client_config(args.bootstrap)
    conf.update({"group.id": "drain-topic", "enable.auto.commit": False, "auto.offset.reset": "error"})
    consumer = Consumer(conf)

    metadata = consumer.list_topics(args.topic, timeout=10).topics[args.topic]
    if metadata.error is not None:
        sys.exit(f"topic {args.topic!r}: {metadata.error}")

    assignment, backlog = [], 0
    for pid in metadata.partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(args.topic, pid), timeout=10)
        start = low if args.start == "earliest" else high
        assignment.append(TopicPartition(args.topic, pid, start))
        backlog += max(0, high - start)
    consumer.assign(assignment)

    print(f"{args.topic}: {backlog} message(s) from {args.start}", file=sys.stderr)
    if args.count_only:
        consumer.close()
        return

    seen = 0
    try:
        while args.follow or seen < backlog:
            if args.max and seen >= args.max:
                break
            msg = consumer.poll(args.timeout)
            if msg is None:
                if not args.follow:
                    break
                continue
            if msg.error():
                print(f"error: {msg.error()}", file=sys.stderr)
                continue
            seen += 1
            key = msg.key().decode(errors="replace") if msg.key() else "-"
            print(f"--- [{seen}] p{msg.partition()}@{msg.offset()} key={key}")
            print(render(msg.value().decode(errors="replace"), args.full))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    print(f"drained {seen} message(s) from {args.topic}", file=sys.stderr)


if __name__ == "__main__":
    main()
