"""Stage 1 of both jobs — everything before the guard, in one operator.

Three things happen, in an order that matters:

1. **drop** control records and empty chunks. A chunk with no text would
   otherwise be enriched, embedded and indexed as an empty document.
2. **normalize** the text — before anything hashes it, so two chunks differing
   only by a soft hyphen, a ligature or a non-breaking space become the same
   chunk to dedup. The original is kept as ``raw_text`` when it changed.
3. **enrich**: the derived fields, the fingerprint (of the *normalized* text,
   which is the point of doing it in this order), the structure flags and the
   quality verdict the guard's gate reads.

Three stages, one operator, and that is deliberate. ``pipeline.full_job`` runs
with ``python.operator-chaining.enabled=false`` — its guard is a
``BroadcastProcessFunction`` and PyFlink's chaining optimizer cannot rewrite a
broadcast-connected operator (``NoSuchFieldException: regularInput``) — so
every Python operator there becomes its own Python worker process. Three
operators would be three processes, per student, for a single pass over one
record. Fusing them costs no clarity because the stages still exist as
functions in :mod:`pipeline.logic.chunk_record` and :mod:`pipeline.logic.enrichment`, which
are Flink-free and unit-tested there; this operator is the list of them.
"""

from __future__ import annotations

import json
import logging
import time

from pyflink.datastream.functions import FlatMapFunction, RuntimeContext

from ..logic.chunk_record import is_chunk, text
from ..logic.enrichment import enrich, normalize_text

logger = logging.getLogger(__name__)


class PrepareFunction(FlatMapFunction):
    """Drop, normalize and enrich. A flat map because step 1 emits nothing."""

    def __init__(self, min_chars: int = 40, drop_low_quality: bool = False) -> None:
        self._min_chars = min_chars
        # The simple pipeline's quality gate. The full one leaves this off and
        # lets the guard decide, because there the threshold is a live policy.
        self._drop_low_quality = drop_low_quality
        self._dropped = None
        self._normalized = None

    def open(self, runtime_context: RuntimeContext) -> None:
        # Per-subtask counters: the only way to find out from the outside that
        # this stage is dropping everything, or normalizing nothing.
        group = runtime_context.get_metrics_group()
        self._dropped = group.counter("dropped_records")
        self._normalized = group.counter("normalized_records")

    def _drop(self) -> list:
        if self._dropped is not None:
            self._dropped.inc()
        return []

    def flat_map(self, value: str):
        payload = json.loads(value)

        if not is_chunk(payload) or not text(payload).strip():
            return self._drop()

        original = text(payload)
        normalized = normalize_text(original)
        if normalized != original:
            payload["raw_text"] = original
            payload["text"] = normalized
            if self._normalized is not None:
                self._normalized.inc()
        payload["normalized"] = normalized != original

        # enrich() fingerprints what it is given, so it has to see the
        # normalized text — that is what makes dedup insensitive to formatting.
        enriched = enrich(payload, min_chars=self._min_chars)
        enriched["ingested_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        if self._drop_low_quality and not enriched.get("keep", True):
            logger.info(
                "DROP %s#%s %s",
                enriched.get("doc_id"), enriched.get("chunk_index"), enriched.get("quality_flags"),
            )
            return self._drop()
        return [json.dumps(enriched, ensure_ascii=False)]
