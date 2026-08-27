"""The control-plane record: rules the running job can be re-configured with.

A compacted Kafka topic carries these, they are broadcast to every subtask, and
the guard operator keeps the latest in ``BroadcastState``. That is the whole
point of the stage — changing which PII detectors run, or how strict the quality
gate is, becomes a produced message instead of an image rebuild and a
``FlinkApplication`` rollout.

Pure and Flink-free so the rules can be validated by a script (and unit-tested)
before anyone publishes them to the topic.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace

from .pii import DEFAULT_ENABLED, DETECTOR_NAMES


@dataclass(frozen=True)
class Policy:
    """Runtime-adjustable knobs. Defaults match a sane out-of-the-box run."""

    # Which PII detectors are active (names from pipeline.logic.pii.DETECTOR_NAMES).
    pii_detectors: frozenset[str] = field(default_factory=lambda: DEFAULT_ENABLED)
    # Redact in the main stream; when False, PII is only reported, not removed.
    pii_redact: bool = True
    # Quality gate.
    min_chars: int = 40
    drop_low_quality: bool = False
    # Documents to ignore entirely (exact match on the record's ``doc_id``).
    blocked_doc_ids: frozenset[str] = field(default_factory=frozenset)
    # Bumped by an operator when the embedding model changes; lands in the index
    # so a reindex can be scoped to documents embedded by an older model.
    embedding_model_version: str = "v1"

    def to_json(self) -> str:
        return json.dumps(
            {
                "pii_detectors": sorted(self.pii_detectors),
                "pii_redact": self.pii_redact,
                "min_chars": self.min_chars,
                "drop_low_quality": self.drop_low_quality,
                "blocked_doc_ids": sorted(self.blocked_doc_ids),
                "embedding_model_version": self.embedding_model_version,
            }
        )


class PolicyError(ValueError):
    """A control-plane message that would misconfigure the job."""


def parse_policy(raw: str, base: Policy | None = None) -> Policy:
    """Apply a control message on top of ``base`` (a partial update is fine).

    Unknown keys and bad values raise, so a typo on the control topic fails
    loudly at the operator instead of silently disabling redaction.
    """
    current = base or Policy()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise PolicyError(f"policy message is not JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise PolicyError("policy message must be a JSON object")

    known = set(Policy.__dataclass_fields__)
    unknown = set(payload) - known
    if unknown:
        raise PolicyError(f"unknown policy keys: {sorted(unknown)}")

    updates: dict = {}
    if "pii_detectors" in payload:
        names = payload["pii_detectors"]
        if not isinstance(names, list) or any(not isinstance(n, str) for n in names):
            raise PolicyError("pii_detectors must be a list of strings")
        bad = set(names) - set(DETECTOR_NAMES)
        if bad:
            raise PolicyError(f"unknown pii detectors: {sorted(bad)} (known: {list(DETECTOR_NAMES)})")
        updates["pii_detectors"] = frozenset(names)
    if "blocked_doc_ids" in payload:
        doc_ids = payload["blocked_doc_ids"]
        if not isinstance(doc_ids, list):
            raise PolicyError("blocked_doc_ids must be a list")
        updates["blocked_doc_ids"] = frozenset(str(d) for d in doc_ids)
    for key in ("pii_redact", "drop_low_quality"):
        if key in payload:
            if not isinstance(payload[key], bool):
                raise PolicyError(f"{key} must be a boolean")
            updates[key] = payload[key]
    if "min_chars" in payload:
        value = payload["min_chars"]
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise PolicyError("min_chars must be a non-negative integer")
        updates["min_chars"] = value
    if "embedding_model_version" in payload:
        updates["embedding_model_version"] = str(payload["embedding_model_version"])

    return replace(current, **updates)
