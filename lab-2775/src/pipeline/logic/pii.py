"""Pure PII detection and redaction (no Flink, no network, no model).

**This is a demonstrator, not a production PII guard.** It exists to make the
side-output and broadcast-policy mechanics of the Flink job concrete with
something recognisable flowing through them. Do not put it in front of a real
corpus and call the result de-identified — see the limits below.

Deliberately regex-and-checksum based rather than an NER model: this runs on
every chunk in the stream, so it has to be microseconds, and the identifiers
that matter most for document ingestion (emails, cards, IBANs) are exactly the
ones with rigid, checkable structure.

What that buys, and what it costs:

* It finds **structured identifiers only**. Names, postal addresses, dates of
  birth, passport numbers and every national identifier without a checksum go
  through untouched. Presidio/spaCy-grade entity detection is a different
  trade-off and belongs behind the async stage, not here.
* Structure is not meaning. Luhn rejects nine of ten arbitrary digit runs, not
  all of them, so a row of figures lifted out of a table can still be redacted
  as a card number. :data:`DEFAULT_ENABLED` is the subset whose false-positive
  rate is low enough to leave on; ``phone`` and ``ip_address`` are off because
  a numeric table row and a version string respectively look just like them.

Two properties the Flink job depends on:

* :func:`redact` never mutates its input and returns *both* the redacted text
  and what was found, so the operator can emit the original to a quarantine
  side output while the main stream carries only the redacted copy.
* the detector set is data (:data:`DETECTORS`), so the broadcast policy stream
  can enable/disable individual detectors at runtime without a redeploy.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Pattern

# --- detectors -------------------------------------------------------------

_EMAIL = re.compile(r"\b[\w.%+-]+@[\w.-]+\.[A-Za-z]{2,}\b")
# Off by default, and this is why: a space counts as a separator, so any row of
# figures ("2019 2020 2021 2022") and any ISO date match it too. Good enough to
# show a policy change taking effect; not good enough to leave on.
_PHONE = re.compile(r"(?:\+\d{1,3}[\s.-]?)?(?:\(\d{2,4}\)[\s.-]?)?\d{2,4}(?:[\s.-]\d{2,4}){2,4}\b")
# Anchored on digits at both ends so the match cannot swallow the separator
# that follows it — "card 4111 1111 1111 1111 and" must not redact the space.
_CARD = re.compile(r"\b\d(?:[ -]?\d){12,18}\b")
_IBAN = re.compile(r"\b[A-Z]{2}\d{2}(?:[ ]?[A-Z0-9]{4}){2,7}(?:[ ]?[A-Z0-9]{1,4})?\b")
_IPV4 = re.compile(r"\b(?:(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\.){3}(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\b")


def _luhn_ok(digits: str) -> bool:
    """Luhn checksum — what separates a card number from any 16-digit run."""
    total = 0
    for i, ch in enumerate(reversed(digits)):
        d = int(ch)
        if i % 2:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def _card_valid(match: str) -> bool:
    digits = re.sub(r"[ -]", "", match)
    return 13 <= len(digits) <= 19 and _luhn_ok(digits)


def _iban_valid(match: str) -> bool:
    """ISO 7064 mod-97: the IBAN's own check digits."""
    compact = re.sub(r"\s", "", match).upper()
    if not 15 <= len(compact) <= 34:
        return False
    rearranged = compact[4:] + compact[:4]
    numeric = "".join(str(int(c, 36)) if c.isalpha() else c for c in rearranged)
    try:
        return int(numeric) % 97 == 1
    except ValueError:
        return False


@dataclass(frozen=True)
class Detector:
    name: str
    pattern: Pattern[str]
    # Second-stage check that rejects structurally-similar non-PII. Without it,
    # every long digit run in a table would be redacted as a card number.
    validator: Callable[[str], bool] | None = None
    placeholder: str = ""

    def token(self) -> str:
        return self.placeholder or f"[REDACTED:{self.name.upper()}]"


DETECTORS: tuple[Detector, ...] = (
    Detector("email", _EMAIL),
    Detector("credit_card", _CARD, _card_valid),
    Detector("iban", _IBAN, _iban_valid),
    Detector("phone", _PHONE),
    Detector("ip_address", _IPV4),
)

# Order matters: cards and IBANs are checked before phone numbers, because the
# phone pattern would otherwise claim a spaced-out card number first.
DETECTOR_NAMES: tuple[str, ...] = tuple(d.name for d in DETECTORS)

DEFAULT_ENABLED: frozenset[str] = frozenset({"email", "credit_card", "iban"})


def detect(text: str, enabled: frozenset[str] | set[str] | None = None) -> list[dict]:
    """Findings as ``{"type", "start", "end", "match"}``, non-overlapping.

    Earlier detectors win an overlap, which is why :data:`DETECTORS` lists the
    checksum-validated ones first.
    """
    if not text:
        return []
    active = DEFAULT_ENABLED if enabled is None else enabled
    findings: list[dict] = []
    taken: list[tuple[int, int]] = []
    for det in DETECTORS:
        if det.name not in active:
            continue
        for m in det.pattern.finditer(text):
            if det.validator and not det.validator(m.group()):
                continue
            start, end = m.span()
            if any(start < t_end and end > t_start for t_start, t_end in taken):
                continue
            taken.append((start, end))
            findings.append({"type": det.name, "start": start, "end": end, "match": m.group()})
    findings.sort(key=lambda f: f["start"])
    return findings


_TOKENS = {d.name: d.token() for d in DETECTORS}


def redact(text: str, enabled: frozenset[str] | set[str] | None = None) -> tuple[str, list[dict]]:
    """``(redacted_text, findings)``. The input string is never mutated.

    Findings keep the matched value so the caller can route the *original* to a
    restricted quarantine stream; never log them.
    """
    findings = detect(text, enabled)
    if not findings:
        return text, []
    out = []
    cursor = 0
    for f in findings:
        out.append(text[cursor : f["start"]])
        out.append(_TOKENS[f["type"]])
        cursor = f["end"]
    out.append(text[cursor:])
    return "".join(out), findings


def summarize(findings: list[dict]) -> dict[str, int]:
    """Counts per PII type — safe to log and to put in the index."""
    counts: dict[str, int] = {}
    for f in findings:
        counts[f["type"]] = counts.get(f["type"], 0) + 1
    return counts
