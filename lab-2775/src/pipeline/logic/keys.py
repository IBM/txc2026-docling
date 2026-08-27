"""The key selectors the jobs partition on.

Flink-free and therefore unit-testable, which matters more than the line count
suggests: a key selector that is wrong does not fail, it silently sends related
records to different subtasks — the dedup state stops matching, the embed
batches stop filling, and everything still looks green.

They are module-level functions and classes (never lambdas or closures over job
state) because the job graph is pickled and shipped to the JobManager.
"""

from __future__ import annotations

import json
import zlib


def by_fingerprint(value: str) -> str:
    """Partition on the text fingerprint — the dedup stage's key."""
    return json.loads(value).get("fingerprint") or ""


class Shard:
    """Spread records over N buffers so each batching subtask gets its own
    buffer and its own timer. The key means nothing else.

    ``zlib.crc32`` rather than ``hash()``: Python salts string hashing per
    process, so ``hash()`` would give the same record different keys in
    different TaskManagers and after every restart — which is invisible until
    a batch stops filling or a timer fires for a key nothing else lands on.
    """

    def __init__(self, shards: int) -> None:
        if shards < 1:
            raise ValueError("shards must be >= 1")
        self._shards = shards

    def __call__(self, value: str) -> str:
        payload = json.loads(value)
        ident = payload.get("chunk_id") or payload.get("doc_id") or ""
        return str(zlib.crc32(ident.encode("utf-8")) % self._shards)
