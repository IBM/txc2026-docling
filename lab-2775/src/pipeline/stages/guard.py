"""Stage 2 — PII redaction and the quality gate, driven by broadcast policy.

This is the control-plane stage. The main chunk stream is connected to a
low-volume ``policy-rules`` stream that is *broadcast*: every parallel subtask
receives every rule and keeps the latest in ``BroadcastState``. Publishing one
message to that topic changes redaction behaviour on the next record, with no
restart and no image rebuild.

Three outputs:

* **main**       — the chunk, with PII replaced by placeholders.
* **quarantine** — the untouched original of any chunk that contained PII,
  for a topic with restricted ACLs. This is the audit trail, and it is the
  reason the stage is a ``ProcessFunction`` and not a ``map``.
* **rejected**   — chunks the quality gate dropped, kept visible instead of
  silently discarded.
"""

from __future__ import annotations

import json
import logging

from pyflink.common.typeinfo import Types
from pyflink.datastream import OutputTag
from pyflink.datastream.functions import BroadcastProcessFunction, RuntimeContext
from pyflink.datastream.state import MapStateDescriptor

from ..logic.pii import redact, summarize
from ..logic.policy import Policy, PolicyError, parse_policy

logger = logging.getLogger(__name__)

# Descriptor must be identical on the broadcast() call and inside the operator.
POLICY_DESCRIPTOR = MapStateDescriptor("policy-rules", Types.STRING(), Types.STRING())

QUARANTINE_TAG = OutputTag("pii-quarantine", Types.STRING())
REJECTED_TAG = OutputTag("quality-rejected", Types.STRING())

_POLICY_KEY = "current"


class PolicyGuardFunction(BroadcastProcessFunction):
    """Applies the live :class:`~pipeline.logic.policy.Policy` to every chunk."""

    def __init__(self, default_policy: Policy | None = None) -> None:
        # Serialized into the job graph, so keep it to plain data.
        self._default_json = (default_policy or Policy()).to_json()
        self._redacted = None
        self._rejected = None

    def open(self, runtime_context: RuntimeContext) -> None:
        group = runtime_context.get_metrics_group()
        self._redacted = group.counter("pii_redacted_chunks")
        self._rejected = group.counter("quality_rejected_chunks")

    def _policy(self, ctx) -> Policy:
        """Latest broadcast policy, falling back to the job's default."""
        state = ctx.get_broadcast_state(POLICY_DESCRIPTOR)
        raw = state.get(_POLICY_KEY)
        if not raw:
            raw = self._default_json
        try:
            return parse_policy(raw)
        except PolicyError:
            logger.exception("Broadcast policy is invalid; falling back to defaults")
            return Policy()

    def process_broadcast_element(self, value: str, ctx):
        """A new control message: validate, merge onto the current policy, store."""
        state = ctx.get_broadcast_state(POLICY_DESCRIPTOR)
        current_raw = state.get(_POLICY_KEY) or self._default_json
        try:
            current = parse_policy(current_raw)
            updated = parse_policy(value, current)
        except PolicyError as exc:
            # A bad rule must not take the job down, and must not silently
            # become the active policy either.
            logger.error("Rejecting policy update: %s", exc)
            return
        state.put(_POLICY_KEY, updated.to_json())
        logger.info("Policy updated: %s", updated.to_json())

    def process_element(self, value: str, ctx):
        payload = json.loads(value)
        policy = self._policy(ctx)

        doc_id = payload.get("doc_id") or ""
        if doc_id in policy.blocked_doc_ids:
            payload["rejected_reason"] = "blocked_doc_id"
            yield REJECTED_TAG, json.dumps(payload, ensure_ascii=False)
            return

        text = payload.get("text") or ""
        redacted, findings = redact(text, policy.pii_detectors)
        if findings:
            # The original goes to quarantine *before* it is replaced, and only
            # there. Never log the matched values.
            yield QUARANTINE_TAG, json.dumps(payload, ensure_ascii=False)
            payload["pii_types"] = summarize(findings)
            payload["pii_count"] = len(findings)
            if policy.pii_redact:
                payload["text"] = redacted
                payload["pii_redacted"] = True
            if self._redacted is not None:
                self._redacted.inc()
        else:
            payload["pii_count"] = 0
            payload["pii_redacted"] = False

        # Quality gate, with the threshold coming from the live policy rather
        # than from the job's start-up configuration.
        if len(payload.get("text") or "") < policy.min_chars:
            flags = list(payload.get("quality_flags") or [])
            if "too_short" not in flags:
                flags.append("too_short")
            payload["quality_flags"] = flags
            payload["keep"] = False
        if policy.drop_low_quality and not payload.get("keep", True):
            payload["rejected_reason"] = "low_quality"
            if self._rejected is not None:
                self._rejected.inc()
            yield REJECTED_TAG, json.dumps(payload, ensure_ascii=False)
            return

        payload["embedding_model_version"] = policy.embedding_model_version
        yield json.dumps(payload, ensure_ascii=False)
