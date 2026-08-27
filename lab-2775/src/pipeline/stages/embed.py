"""Stage 4 of both jobs — embed, in timer-bounded micro-batches.

Embedding is a network call to watsonx.ai, not a model in this process
(:mod:`pipeline.watsonx`), and the round trip costs the same whether it carries
one short chunk or thirty of them. One request per chunk would be a thousand
round trips per student per run.

Batching in a stream needs a way to stop waiting, and that is what a Flink
timer is for. Records accumulate in keyed state and flush on whichever comes
first:

* ``batch_size`` records, or
* ``max_delay_ms`` of processing time since the batch opened.

The stream is keyed by a shard of the chunk id (:class:`pipeline.logic.keys.Shard`)
purely so each subtask gets its own buffer and its own timer — the key carries
no meaning beyond that.
"""

from __future__ import annotations

import json
import logging

from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor

from ..config import EmbeddingConfig, WatsonxConfig

logger = logging.getLogger(__name__)


class EmbedFunction(KeyedProcessFunction):
    """One watsonx.ai request per batch, flushed on size or on the timer."""

    def __init__(
        self,
        config: EmbeddingConfig,
        batch_size: int = 32,
        max_delay_ms: int = 200,
        watsonx: WatsonxConfig | None = None,
    ) -> None:
        self._config = config
        self._batch_size = batch_size
        self._max_delay_ms = max_delay_ms
        self._watsonx = watsonx or WatsonxConfig()
        self._client = None
        self._buffer = None
        self._timer = None
        self._batches = None
        self._records = None

    def open(self, runtime_context: RuntimeContext) -> None:
        from ..watsonx import WatsonxEmbeddings

        self._client = WatsonxEmbeddings(self._watsonx, self._config)
        self._client.open()
        self._buffer = runtime_context.get_list_state(ListStateDescriptor("embed-buffer", Types.STRING()))
        self._timer = runtime_context.get_state(ValueStateDescriptor("embed-timer", Types.LONG()))
        group = runtime_context.get_metrics_group()
        self._batches = group.counter("embed_batches")
        self._records = group.counter("embed_records")

    def process_element(self, value: str, ctx):
        self._buffer.add(value)
        pending = list(self._buffer.get())
        if len(pending) >= self._batch_size:
            self._cancel_timer(ctx)
            yield from self._flush(pending)
            return
        if self._timer.value() is None:
            deadline = ctx.timer_service().current_processing_time() + self._max_delay_ms
            ctx.timer_service().register_processing_time_timer(deadline)
            self._timer.update(deadline)

    def on_timer(self, timestamp: int, ctx):
        self._timer.clear()
        pending = list(self._buffer.get())
        if pending:
            yield from self._flush(pending)

    def _cancel_timer(self, ctx) -> None:
        deadline = self._timer.value()
        if deadline is not None:
            ctx.timer_service().delete_processing_time_timer(deadline)
            self._timer.clear()

    def _flush(self, pending: list[str]):
        self._buffer.clear()
        payloads = [json.loads(p) for p in pending]
        texts = [p.get("text") or "" for p in payloads]
        vectors = self._client.embed(texts)
        if self._batches is not None:
            self._batches.inc()
            self._records.inc(len(payloads))
        for payload, vector in zip(payloads, vectors):
            payload["embedding"] = vector
            payload["embedding_dim"] = len(payload["embedding"])
            yield json.dumps(payload, ensure_ascii=False)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
