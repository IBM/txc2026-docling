"""The terminal operators — one per name in ``SINK_TYPE`` that needs Python.

* ``log``        — :class:`LogSinkFunction` writes a compact line per chunk to
  the TaskManager log, enough to verify the whole path from a `docker logs`.
* ``none``       — :class:`DiscardSinkFunction` counts records and drops them,
  for measuring the graph without any output at all.
* ``opensearch`` — :class:`OpenSearchSinkFunction` indexes the chunk with its
  vector, through ``opensearch-py`` rather than the JVM connector so the whole
  sink stays in Python and can carry a ``knn_vector`` field without a Java
  schema. Imported lazily by :func:`pipeline.stages.sink`, because the client
  library is only installed where it is used.

``kafka`` needs no function here: the records go to a topic through a real
``KafkaSink`` and ``scripts/drain_topic.py`` reads them back.

``SINK_TYPE`` names a *list*, so these are not alternatives — the workshop runs
``kafka,opensearch`` and every finished record goes to both. :func:`pipeline.graph.sink`
is what attaches them, and it attaches each to the same stream, so the fan-out
moves no records between subtasks.
"""

from __future__ import annotations

import json
import logging

from pyflink.datastream.functions import MapFunction, RuntimeContext

from ..config import OpenSearchConfig
from ..logic import chunk_record

logger = logging.getLogger("pipeline.sink")


class LogSinkFunction(MapFunction):
    def map(self, value: str) -> str:
        payload = json.loads(value)
        emb = payload.get("embedding") or []
        summary = {
            "doc_id": chunk_record.doc_id(payload),
            "chunk_index": payload.get("chunk_index"),
            "chunk_id": payload.get("chunk_id"),
            "filename": chunk_record.filename(payload),
            "char_count": payload.get("char_count"),
            "token_count": payload.get("token_count"),
            "quality_flags": payload.get("quality_flags"),
            "headings": chunk_record.headings(payload),
            "page_numbers": chunk_record.page_numbers(payload),
            "text_preview": chunk_record.text(payload)[:160],
            "embedding_dim": len(emb),
            "embedding_head": [round(float(x), 4) for x in emb[:4]],
        }
        logger.info("CHUNK %s", json.dumps(summary, ensure_ascii=False))
        return json.dumps(summary)


class DiscardSinkFunction(MapFunction):
    """Count and drop. The graph still runs end to end, nothing is written."""

    def __init__(self, log_every: int = 100) -> None:
        self._log_every = log_every
        self._seen = 0
        self._counter = None

    def open(self, runtime_context) -> None:
        self._counter = runtime_context.get_metrics_group().counter("discarded_records")

    def map(self, value: str) -> str:
        self._seen += 1
        if self._counter is not None:
            self._counter.inc()
        if self._log_every and self._seen % self._log_every == 0:
            logger.info("discarded %d records", self._seen)
        return value


# Bookkeeping for the stream, not worth storing in the index.
_INTERNAL_FIELDS = ("duplicate", "raw_text")


class OpenSearchSinkFunction(MapFunction):
    def __init__(self, config: OpenSearchConfig) -> None:
        self._config = config
        self._client = None

    def open(self, runtime_context: RuntimeContext) -> None:
        from opensearchpy import OpenSearch

        self._client = OpenSearch(**self._config.client_kwargs())

    def map(self, value: str) -> str:
        payload = json.loads(value)
        # The producer's chunk_id is the document id: deterministic and
        # content-addressed, so re-ingesting a document upserts rather than
        # duplicating. Fall back only for a record that arrived without one.
        doc_id = payload.get(chunk_record.CHUNK_ID) or chunk_record.stable_chunk_id(
            chunk_record.binary_hash(payload),
            chunk_record.doc_id(payload),
            chunk_record.chunk_index(payload),
        )
        # Pass the record through as-is (minus stream bookkeeping) so enrichment
        # fields land in the index without this sink knowing about them.
        body = {k: v for k, v in payload.items() if k not in _INTERNAL_FIELDS}
        self._client.index(index=self._config.index, id=doc_id, body=body)
        logger.debug("Indexed %s into %s", doc_id, self._config.index)
        return doc_id

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
