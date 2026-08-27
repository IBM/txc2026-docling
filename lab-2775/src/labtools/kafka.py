"""Shared Kafka client setup for the laptop-side scripts.

Works against both targets without code changes:

* **local stack** — ``localhost:29092``, PLAINTEXT, no credentials (the default
  when nothing is configured).
* **hosted test system / Confluent Cloud** — SASL_SSL + PLAIN, driven by
  the ``KAFKA_*`` variables :mod:`labtools.config` resolves out of ``lab.yaml``.

The compose stack's own configuration holds *in-compose* addresses
(``kafka:9092``) which do not resolve from the host — from a laptop use
``localhost:29092`` (the default here) or pass ``--bootstrap``.
"""

from __future__ import annotations

import os
import sys

DEFAULT_BOOTSTRAP = "localhost:29092"


def client_config(bootstrap: str | None = None) -> dict:
    servers = bootstrap or os.environ.get("KAFKA_BOOTSTRAP_SERVERS") or DEFAULT_BOOTSTRAP
    api_key = os.environ.get("KAFKA_API_KEY", "")
    api_secret = os.environ.get("KAFKA_API_SECRET", "")
    # Credentials present -> assume the secured listener unless told otherwise.
    default_protocol = "SASL_SSL" if api_key and api_secret else "PLAINTEXT"
    protocol = os.environ.get("KAFKA_SECURITY_PROTOCOL", default_protocol).upper()

    conf = {"bootstrap.servers": servers, "security.protocol": protocol}
    if protocol.startswith("SASL"):
        if not (api_key and api_secret) or api_secret.startswith("CHANGEME"):
            sys.exit("kafka.api_key / kafka.api_secret are required for SASL — see lab.yaml")
        conf.update(
            {
                "sasl.mechanism": os.environ.get("KAFKA_SASL_MECHANISM", "PLAIN"),
                "sasl.username": api_key,
                "sasl.password": api_secret,
            }
        )
    if protocol.endswith("SSL"):
        ca = os.environ.get("KAFKA_CA_LOCATION")
        if ca:
            conf["ssl.ca.location"] = ca
    return conf


def require_confluent_kafka():
    try:
        import confluent_kafka  # noqa: F401
    except ImportError:
        sys.exit("confluent-kafka is not installed — pip install confluent-kafka")
    return confluent_kafka


def ensure_topic(conf: dict, topic: str, partitions: int = 3, replication: int | None = None) -> None:
    """Create ``topic`` if missing (the brokers here do not auto-create)."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient(conf)
    if topic in admin.list_topics(timeout=10).topics:
        return
    if replication is None:
        # One broker locally, three on the hosted cluster.
        replication = 1 if len(admin.list_topics(timeout=10).brokers) < 3 else 3
    for name, fut in admin.create_topics([NewTopic(topic, partitions, replication)]).items():
        try:
            fut.result()
            print(f"created topic {name!r} (partitions={partitions}, rf={replication})")
        except Exception as exc:  # noqa: BLE001
            if "already exists" not in str(exc).lower():
                raise
