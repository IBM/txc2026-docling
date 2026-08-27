"""Stage 3 of both jobs — exact duplicates, dropped where they are detected.

What makes this a Flink stage rather than a script: the state is keyed by the
text fingerprint, checkpointed with the job, and expires on its own. Docling's
target is an at-least-once producer and a student re-uploading the same file is
a duplicate by construction, so this fires in the workshop rather than in
theory.

It drops rather than tags. A tag-then-filter pair would put the decision in the
record where it can be read, but it is a second operator — and in
``pipeline.full_job`` a second operator is a second Python worker process (see
:mod:`pipeline.stages.prepare`). The counter below is the evidence instead.
"""

from __future__ import annotations

import json
import logging

from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext

logger = logging.getLogger(__name__)


class DeduplicateFunction(KeyedProcessFunction):
    """Runs after ``key_by(fingerprint)``: first occurrence passes, repeats stop
    here. The state is per key, so identical text from two different documents
    collapses to one indexed chunk."""

    def __init__(self, ttl_hours: int = 24) -> None:
        self._ttl_hours = ttl_hours
        self._seen = None
        self._dropped = None

    def open(self, runtime_context: RuntimeContext) -> None:
        from pyflink.common.time import Time
        from pyflink.common.typeinfo import Types
        from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

        descriptor = ValueStateDescriptor("seen-fingerprint", Types.BOOLEAN())
        # Without a TTL the fingerprint set grows forever on an unbounded stream.
        descriptor.enable_time_to_live(
            StateTtlConfig.new_builder(Time.hours(self._ttl_hours))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .cleanup_incrementally(100, True)
            .build()
        )
        self._seen = runtime_context.get_state(descriptor)
        # Nothing is written to a topic when a duplicate is dropped, so this
        # counter is the evidence that it happened. The other half of the
        # evidence is external and needs no code: the input topic advances and
        # the output topic does not.
        self._dropped = runtime_context.get_metrics_group().counter("duplicate_chunks")

    def process_element(self, value: str, ctx: "KeyedProcessFunction.Context"):
        if self._seen.value():
            payload = json.loads(value)
            logger.info("DUP %s#%s", payload.get("doc_id"), payload.get("chunk_index"))
            if self._dropped is not None:
                self._dropped.inc()
            return []
        self._seen.update(True)
        return [value]
