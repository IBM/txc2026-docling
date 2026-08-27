"""How a chunk record is printed — on a terminal and in the dashboard.

The record layout is the same at every stage of the pipeline: the chunk record
Docling wrote, with the stages' derived fields added. So one summary line works
on the input topic, the output topic and both side-output topics, and the same
function backs ``scripts/drain_topic.py`` and the inspector's Messages tab —
which is the point of it living here rather than inside either of them.
"""

from __future__ import annotations

import json

# Long vector fields are noise on a terminal; summarize them instead.
_VECTOR_FIELDS = ("embedding",)


def summarize(payload: dict) -> str:
    """One compact line per record."""
    text = (payload.get("text") or payload.get("text_preview") or "").replace("\n", " ")
    doc_id = payload.get("doc_id") or "?"

    # Anything a foreign producer put on the topic. The jobs drop these at
    # `prepare`; printing them as chunks would be a lie.
    if payload.get("kind"):
        return f"{payload['kind']} {doc_id}"

    bits = [
        f"{doc_id}#{payload.get('chunk_index', '?')}",
        f"chars={payload.get('char_count', len(text))}",
    ]
    if payload.get("token_count") is not None:
        bits.append(f"tokens={payload['token_count']}")
    if payload.get("page_numbers"):
        bits.append(f"pages={payload['page_numbers']}")
    if payload.get("headings"):
        bits.append(f"heading={payload['headings'][-1]!r}")
    if payload.get("content_type"):
        bits.append(f"type={payload['content_type']}")
    if payload.get("quality_flags"):
        bits.append(f"flags={payload['quality_flags']}")
    if payload.get("pii_count"):
        bits.append(f"pii={payload.get('pii_types') or payload['pii_count']}")
    if payload.get("rejected_reason"):
        bits.append(f"rejected={payload['rejected_reason']}")
    if payload.get("embedding"):
        bits.append(f"dim={len(payload['embedding'])}")
    elif payload.get("embedding_dim"):
        bits.append(f"dim={payload['embedding_dim']}")
    return "  ".join(bits) + f"\n    {text[:160]}"


def render(raw: str, full: bool) -> str:
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return raw
    if not isinstance(payload, dict):
        return raw
    if not full:
        return summarize(payload)
    shown = dict(payload)
    for fieldname in _VECTOR_FIELDS:
        vector = shown.get(fieldname)
        if isinstance(vector, list):
            shown[fieldname] = f"<{len(vector)} floats: {[round(float(x), 4) for x in vector[:4]]}...>"
    return json.dumps(shown, indent=2, ensure_ascii=False)
