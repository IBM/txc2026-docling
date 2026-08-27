"""Read-only probes behind the pipeline inspector.

The probes deliberately reuse the pipeline's own configuration dataclasses and
the lab's own connection helpers (:mod:`labtools.kafka`) rather than restating
them — every package under ``src/`` imports by name, so there is no path setup
to do here.
"""

from . import settings  # imported first: it is what resolves the configuration

__all__ = ["settings", "kafka_probe", "flink_probe", "opensearch_probe", "rag", "topology",
           "render", "brand"]
