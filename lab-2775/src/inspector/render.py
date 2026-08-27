"""Turning probe results into a picture: the DAG, and what a record looks like.

The graph is emitted as DOT and rendered in the browser by Streamlit, so no
system Graphviz install is needed — one less thing for a student to get wrong
before the workshop starts.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from labtools import records

from .topology import SINK, TOPIC, Topology

# --- how a node is doing ----------------------------------------------------
# Six states, and the distinction that matters is the last two: a stage with a
# backlog that is *not* moving is stalled, and looks nothing like an idle stage
# with an empty queue, even though both show zero throughput.
FLOWING, BUSY, IDLE, BACKLOG, OFF, UNKNOWN = "flowing", "busy", "idle", "backlog", "off", "unknown"
# A seventh, which is not a measurement: the services upstream of Kafka (the COS
# bucket and Docling) are outside anything this dashboard can
# probe, so they get their own colour rather than borrowing "idle" — which would
# claim they are doing nothing.
EXTERNAL = "external"

_STYLE = {
    EXTERNAL: ("#3b5bdb", "#e7ecff", "#1c2f7a"),
    FLOWING: ("#1e7a3c", "#c9f0d5", "#0f3d1f"),
    BUSY:    ("#a1620a", "#ffe6b0", "#5a3600"),
    BACKLOG: ("#9a3412", "#ffd9c7", "#5c1f0a"),
    IDLE:    ("#94a3b8", "#eef2f7", "#334155"),
    OFF:     ("#cbd5e1", "#f8fafc", "#94a3b8"),
    UNKNOWN: ("#94a3b8", "#f1f5f9", "#475569"),
}

STATE_LABEL = {
    EXTERNAL: "🔵 external service",
    FLOWING: "🟢 flowing",
    BUSY: "🟠 busy",
    BACKLOG: "🔴 backlog, not moving",
    IDLE: "⚪ idle",
    OFF: "⚫ disabled",
    UNKNOWN: "◻︎ unknown",
}


@dataclass
class NodeState:
    status: str = UNKNOWN
    lines: tuple[str, ...] = ()      # extra label lines (counts, rates, busy %)
    tooltip: str = ""


def _esc(text: str) -> str:
    return str(text).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def dot(
    topo: Topology,
    states: dict[str, NodeState],
    show_side: bool = True,
    show_ingest: bool = True,
    rankdir: str = "LR",
) -> str:
    """The job's stage graph as DOT, coloured by what each stage is doing.

    ``rankdir`` is worth a thought rather than a default: the two-vertex enrich
    job reads well left to right, while the v2 graph is two dozen stages and
    only fits a laptop screen top to bottom.
    """
    out = [
        "digraph pipeline {",
        f'  rankdir={rankdir};',
        '  bgcolor="transparent";',
        '  ranksep=0.45; nodesep=0.28;',
        '  node [fontname="Helvetica,Arial,sans-serif" fontsize=12 style="filled,rounded" '
        'shape=box margin="0.18,0.10" penwidth=1.6];',
        '  edge [fontname="Helvetica,Arial,sans-serif" fontsize=10 color="#8fa0b5" '
        'arrowsize=0.8];',
    ]

    drawn = set()
    for node in topo.nodes:
        if not show_side and node.group == "side":
            continue
        # Three more nodes across is a third less room for the Flink graph, so
        # the path in front of Kafka can be folded away once it is understood.
        if not show_ingest and node.group == "ingest":
            continue
        state = states.get(node.id, NodeState(OFF if not node.enabled else UNKNOWN))
        status = OFF if not node.enabled else state.status
        border, fill, text = _STYLE[status]
        label = "\\n".join([_esc(node.label), *(_esc(line) for line in state.lines)])
        shape = node.shape or ("cylinder" if node.kind in (TOPIC, SINK) and node.topic else "box")
        extra = ' style="filled,rounded,dashed"' if status == OFF else ""
        # A clickable node where there is something to click: the bucket's
        # console, Docling's endpoint. Graphviz turns it into an <a> in the SVG.
        if node.url:
            extra += f' URL="{_esc(node.url)}" target="_blank"'
        tooltip = _esc(state.tooltip or node.why)
        out.append(
            f'  "{node.id}" [label="{label}" shape={shape} color="{border}" '
            f'fillcolor="{fill}" fontcolor="{text}" tooltip="{tooltip}"{extra}];'
        )
        drawn.add(node.id)

    for edge in topo.edges:
        if edge.src not in drawn or edge.dst not in drawn:
            continue
        style = ' style=dashed color="#b4c0d0"' if edge.side else ""
        label = f' label="{_esc(edge.label)}"' if edge.label else ""
        out.append(f'  "{edge.src}" -> "{edge.dst}" [{label.strip()}{style}];')

    out.append("}")
    return "\n".join(out)


def legend_markdown() -> str:
    return " · ".join(STATE_LABEL[s] for s in (FLOWING, BUSY, BACKLOG, IDLE, OFF, EXTERNAL, UNKNOWN))


# --- records ---------------------------------------------------------------
def summarize_record(raw: str) -> str:
    """One line per record, using the lab's own summariser.

    :mod:`labtools.records` already knows the shape of every record on every
    topic — chunk, document summary, tombstone — and so does
    ``scripts/drain_topic.py``, because it is the same function. The dashboard
    reuses it instead of growing a second, divergent opinion about the layout.
    """
    return records.render(raw, full=False)


def record_fields(raw: str) -> dict:
    """The record with its vectors summarized, ready to show as JSON."""
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {"_raw": raw}
    if not isinstance(payload, dict):
        return {"_value": payload}
    shown = dict(payload)
    for key, value in payload.items():
        if isinstance(value, list) and len(value) > 12 and all(isinstance(x, (int, float)) for x in value):
            head = [round(float(x), 4) for x in value[:4]]
            shown[key] = f"<{len(value)} floats: {head}…>"
    return shown


def record_row(raw: str) -> dict:
    """A few columns worth putting in a table, whatever kind of record it is."""
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {"kind": "raw", "text": raw[:120]}
    if not isinstance(payload, dict):
        return {"kind": "raw", "text": str(payload)[:120]}

    kind = payload.get("kind") or ("tombstone" if payload.get("op") == "delete" else "chunk")
    text = (payload.get("text") or payload.get("text_preview") or "").replace("\n", " ")
    embedding = payload.get("embedding")
    # Empty strings rather than None: a column of Nones renders as the literal
    # word "None" in the table, which reads like a value the record carries.
    def blank(value):
        return "" if value is None else value

    return {
        "kind": kind,
        "doc_id": (payload.get("doc_id") or "")[:16],
        "chunk": blank(payload.get("chunk_index")),
        "chars": blank(payload.get("char_count") or len(text) or None),
        "tokens": blank(payload.get("token_count")),
        "type": blank(payload.get("content_type")),
        "flags": ",".join(payload.get("quality_flags") or []),
        "dim": blank(len(embedding) if isinstance(embedding, list) else payload.get("embedding_dim")),
        "heading": (payload.get("headings") or [""])[-1][:40],
        "text": text[:120],
    }
