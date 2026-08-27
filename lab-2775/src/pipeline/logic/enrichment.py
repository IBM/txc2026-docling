"""Pure helpers for chunk records: normalization, and the derived fields.

No Flink and no network here, so everything is unit-testable and behaves
identically in an operator and in a laptop-side script.

Two things live here, in the order the pipeline applies them:

**normalize_text** — the unglamorous step that decides retrieval quality.
PDF-derived text arrives with soft hyphens splitting words across lines,
ligatures, non-breaking spaces, zero-width joiners and repeated page furniture.
Normalizing *before* the fingerprint is computed is what makes dedup see "the
same paragraph" as the same paragraph.

**enrich** — the "cheap facts" you can derive from a chunk without another model
call, and which retrieval systems routinely filter, boost or budget on:

* **size**       — ``char_count`` / ``word_count`` / ``token_count`` drive
  context-window budgeting at query time and catch degenerate chunks.
* **identity**   — ``fingerprint`` (hash of the normalized text) is the key for
  cross-document dedup. The stable document id, ``chunk_id``, is *not* derived
  here: the producer stamps it (see :mod:`pipeline.logic.chunk_record`).
* **structure**  — ``heading_path`` / ``section_depth`` / ``page_start`` make
  citations and section-scoped filters possible.
* **content type** — ``has_table`` / ``has_code`` / ``has_formula`` / ``has_list``
  let a retriever route or boost (e.g. table chunks for numeric questions).
* **quality**    — ``alpha_ratio`` / ``digit_ratio`` / ``symbol_ratio`` and the
  resulting ``quality_flags`` are the standard way to drop OCR noise, page
  furniture and near-empty fragments before they pollute the index.
* **script**     — coarse writing-system detection; a cheap stand-in for full
  language ID that costs nothing and is enough to route or filter for a
  multilingual embedding model.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from typing import Any

from . import chunk_record

_WS = re.compile(r"\s+")
_WORD = re.compile(r"\w+", re.UNICODE)
_TABLE_ROW = re.compile(r"^\s*\|.*\|\s*$", re.MULTILINE)
_LIST_ITEM = re.compile(r"^\s*([-*+]|\d+[.)])\s+", re.MULTILINE)
_CODE_FENCE = re.compile(r"```|^\s{4,}\S", re.MULTILINE)
_FORMULA = re.compile(r"\$[^$\n]+\$|\\\(|\\\[|\\begin\{(equation|align)")

# Coarse writing-system buckets, by unicodedata.name prefix.
_SCRIPTS = ("LATIN", "CYRILLIC", "GREEK", "ARABIC", "HEBREW", "DEVANAGARI", "HIRAGANA", "KATAKANA", "HANGUL", "CJK")


# --- normalization ---------------------------------------------------------

# Word split across a line break: "inter-\nnational" -> "international".
# Only joined when both sides are letters, so "10-\n20" (a range) is left alone.
# \u00ad (the soft hyphen) is what PDF extractors leave behind. Every literal
# non-ASCII character in the three patterns below is written as an escape:
# they are invisible by definition, so in the source they are unreadable, and
# U+202A..U+202E in particular are the "Trojan Source" bidi overrides that
# secret scanners flag on sight. The pattern is what strips them.
_DEHYPHEN = re.compile(r"(?<=[^\W\d_])[-\u00ad]\s*\n\s*(?=[^\W\d_])")
# C0/C1 control characters, except tab and newline which are handled separately.
_CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")
# Zero-width and bidi marks that survive PDF extraction and break tokenizers.
_INVISIBLE = re.compile(r"[\u200b-\u200f\u202a-\u202e\ufeff\u00ad]")
_MULTI_NL = re.compile(r"\n{3,}")
_TRAILING_WS = re.compile(r"[ \t]+(?=\n)")
# Runs of any horizontal space (incl. NBSP and the Unicode spaces) collapse to one.
_SPACES = re.compile(r"[ \t\u00a0\u2000-\u200a\u202f\u205f\u3000]+")

# Page furniture: a line that is just a number, "Page 3", "3 of 12", or a rule.
_PAGE_FURNITURE = re.compile(
    r"^\s*(?:page\s+)?\d+\s*(?:/|of|\|)?\s*\d*\s*$|^\s*[-_=~—]{3,}\s*$",
    re.IGNORECASE,
)


def normalize_text(text: str) -> str:
    """Canonical form of a chunk's text, safe to embed and to hash.

    NFKC folds ligatures and full-width forms into their plain equivalents,
    which matters because the tokenizer and the fingerprint should not see
    "ﬁnance" and "finance" as different words.
    """
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = _DEHYPHEN.sub("", text)
    text = _INVISIBLE.sub("", text)
    text = _CONTROL.sub(" ", text)
    text = "\n".join(line for line in text.split("\n") if not _PAGE_FURNITURE.match(line))
    text = _SPACES.sub(" ", text)
    text = _TRAILING_WS.sub("", text)
    text = _MULTI_NL.sub("\n\n", text)
    return text.strip()


# --- derived fields --------------------------------------------------------


def _canonical(text: str) -> str:
    """Whitespace- and case-folded form, hashed by :func:`fingerprint`.

    Not :func:`normalize_text`: that one repairs the text and the repair is
    kept; this one is thrown away as soon as it has been hashed.
    """
    return _WS.sub(" ", text).strip().casefold()


def fingerprint(text: str) -> str:
    """Stable 32-hex digest of the normalized text — the dedup key."""
    return hashlib.sha256(_canonical(text).encode()).hexdigest()[:32]


def detect_script(text: str, sample: int = 400) -> str:
    """Dominant writing system of the first ``sample`` letters ("latin", "cjk", …)."""
    counts: dict[str, int] = {}
    seen = 0
    for ch in text:
        if not ch.isalpha():
            continue
        try:
            name = unicodedata.name(ch)
        except ValueError:
            continue
        for script in _SCRIPTS:
            if name.startswith(script):
                counts[script.lower()] = counts.get(script.lower(), 0) + 1
                break
        seen += 1
        if seen >= sample:
            break
    if not counts:
        return "unknown"
    return max(counts.items(), key=lambda kv: kv[1])[0]


def _ratios(text: str) -> dict[str, float]:
    total = len(text) or 1
    alpha = digit = space = 0
    for ch in text:
        if ch.isalpha():
            alpha += 1
        elif ch.isdigit():
            digit += 1
        elif ch.isspace():
            space += 1
    symbol = total - alpha - digit - space
    return {
        "alpha_ratio": round(alpha / total, 4),
        "digit_ratio": round(digit / total, 4),
        "symbol_ratio": round(symbol / total, 4),
    }


def quality_flags(char_count: int, word_count: int, ratios: dict[str, float], min_chars: int = 40) -> list[str]:
    """Reasons this chunk is probably not worth retrieving.

    An empty list means the chunk looks like normal prose/tabular content.
    """
    flags: list[str] = []
    if char_count < min_chars:
        flags.append("too_short")
    if ratios["alpha_ratio"] < 0.3:
        flags.append("low_alpha")  # OCR noise, page furniture, pure numbers
    if ratios["symbol_ratio"] > 0.4:
        flags.append("high_symbol")
    if word_count and char_count / word_count > 25:
        flags.append("long_tokens")  # base64 blobs, broken de-hyphenation
    return flags


def content_type(record: dict[str, Any]) -> str:
    """One label for what a chunk mostly *is*, from the structure flags above.

    Ordered by how much the label changes what a consumer should do with the
    chunk: code first (a sentence-embedding model is a poor fit for a stack
    trace), then table, formula, list, and prose as the default.
    """
    for flag, label in (("has_code", "code"), ("has_table", "table"),
                        ("has_formula", "formula"), ("has_list", "list")):
        if record.get(flag):
            return label
    return "prose"


def enrich(record: dict[str, Any], *, min_chars: int = 40) -> dict[str, Any]:
    """Return the chunk record plus derived fields. The input is not mutated.

    Nothing in the record is renamed or moved: ``doc_id``, ``metadata``,
    ``chunk_id`` and the rest stay exactly as the producer wrote them, and the
    derived fields are added alongside. ``chunk_id`` in particular is the
    producer's — it is deterministic and content-addressed there, and the dedup
    stage and the index both collapse on it.
    """
    text = chunk_record.text(record)
    headings = chunk_record.headings(record)
    pages = chunk_record.page_numbers(record)
    words = _WORD.findall(text)
    char_count = len(text)
    word_count = len(words)
    ratios = _ratios(text)
    flags = quality_flags(char_count, word_count, ratios, min_chars=min_chars)

    out = dict(record)
    out.update(
        {
            "fingerprint": fingerprint(text),
            "char_count": char_count,
            "word_count": word_count,
            "avg_word_len": round(sum(len(w) for w in words) / word_count, 2) if word_count else 0.0,
            "heading_path": " > ".join(headings),
            "section_depth": len(headings),
            "page_start": pages[0] if pages else None,
            "page_end": pages[-1] if pages else None,
            "has_table": len(_TABLE_ROW.findall(text)) >= 2,
            "has_list": bool(_LIST_ITEM.search(text)),
            "has_code": bool(_CODE_FENCE.search(text)),
            "has_formula": bool(_FORMULA.search(text)),
            "script": detect_script(text),
            "quality_flags": flags,
            "keep": not flags,
            **ratios,
        }
    )
    out["content_type"] = content_type(out)
    if not out.get(chunk_record.CHUNK_ID):
        # Only for a producer that omitted it; the real ones always send one.
        out[chunk_record.CHUNK_ID] = chunk_record.stable_chunk_id(
            chunk_record.binary_hash(record),
            chunk_record.doc_id(record),
            chunk_record.chunk_index(record),
        )
    if out.get("token_count") is None:
        # Rough stand-in: the converter reports no token count. ~4 chars/token.
        out["token_count"] = round(char_count / 4)
        out["token_count_estimated"] = True
    return out
