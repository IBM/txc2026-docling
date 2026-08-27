"""Pipeline inspector — a local window onto the COS → Docling → Kafka → Flink system.

Run it from the project root::

    ./pipeline.sh inspect       # creates the venv, then starts Streamlit

Nothing here writes: it reads topic watermarks, asks for committed offsets,
reads Flink's or CMF's status, and reads documents out of OpenSearch. No
consumer group is joined and no offset is committed, so refreshing this page
cannot move a running job's offsets or trigger a rebalance.
"""

from __future__ import annotations

import os
import time
from collections import deque

import streamlit as st

from inspector import brand
from inspector import deployment as deployment_mod
from inspector import flink_probe, kafka_probe, opensearch_probe, rag as rag_mod, render, settings as settings_mod
from inspector import topology as topology_mod
from inspector.render import BACKLOG, BUSY, EXTERNAL, FLOWING, IDLE, OFF, UNKNOWN, NodeState
from pipeline.config import GenerationConfig

# The lab this dashboard belongs to. It is the name on the workshop's schedule,
# so it is the name at the top of the page: a student who has three tabs open
# should be able to tell which one is the lab.
LAB_ID = "LAB-2775"
LAB_TITLE = "From Bucket to RAG"
LAB_SUBTITLE = "Event-Driven Docling for watsonx and Confluent"

st.set_page_config(page_title=f"{LAB_ID} · {LAB_TITLE}", page_icon="🔎", layout="wide")

RATE_WINDOW_S = 90.0        # how far back a throughput figure looks
SECRET_KEYS = ("KAFKA_API_SECRET", "OPENSEARCH_PASSWORD", "CMF_AUTH", "DOCLING_SERVICE_API_KEY",
               "WATSONX_APIKEY")


# --------------------------------------------------------------- plumbing --
@st.cache_resource(show_spinner=False)
def _reader(signature: str):
    return kafka_probe.make_reader()


@st.cache_resource(show_spinner=False)
def _group_probe(signature: str, group: str):
    return kafka_probe.make_group_probe(group)


@st.cache_resource(show_spinner=False)
def _admin(signature: str):
    return kafka_probe.make_admin()


@st.cache_resource(show_spinner=False)
def _opensearch(signature: str, hosts: str, user: str, password: str, verify: bool, ca: str):
    return opensearch_probe.connect(hosts, user, password, verify, ca)


def opensearch_client():
    """The one connection the OpenSearch and Ask tabs share.

    Both need it and both are rendered on every rerun, so it is built once and
    cached on the values it was built from — a changed password or a switched
    profile makes a new one, a slider move does not.
    """
    return _opensearch(
        signature, cfg.opensearch_hosts, cfg.get("OPENSEARCH_USERNAME"),
        cfg.get("OPENSEARCH_PASSWORD"), cfg.opensearch_verify, cfg.opensearch_ca,
    )


def rate_of(key: str, value: int, now: float) -> float | None:
    """Messages per second for ``key``, from samples taken across reruns.

    Kept over the *end* offset rather than the retained count: retention can
    delete messages from the tail, and a topic losing old records is not the
    same event as a stage falling behind.
    """
    series = st.session_state.setdefault("history", {}).setdefault(key, deque(maxlen=240))
    if not series or series[-1][0] != now:
        series.append((now, value))
    while len(series) > 2 and now - series[0][0] > RATE_WINDOW_S:
        series.popleft()
    if len(series) < 2:
        return None
    t0, v0 = series[0]
    span = now - t0
    return None if span < 1.0 else max(0.0, (value - v0) / span)


def fmt_rate(rate: float | None) -> str:
    if rate is None:
        return "–"
    if rate == 0:
        return "0/s"
    return f"{rate:.2f}/s" if rate < 10 else f"{rate:.0f}/s"


# ------------------------------------------------------------ the sidebar --
# The conference mark sits above the sidebar on every rerun. The dark-on-light
# variant, because the page itself is light — the black treatment is the header's.
if (_sidebar_logo := brand.ASSETS / brand.LOGOS["techxchange"].default).exists():
    st.logo(str(_sidebar_logo), size="medium")
st.sidebar.title("🔎 Pipeline inspector")
st.sidebar.caption(f"**{LAB_ID}** · {LAB_TITLE}")

# There is only one target in the lab: the workshop's own cluster. The local
# compose stack is a development thing, not published with the lab, so the
# choice appears only when LAB_DEV=1 — a selector with one working answer is a
# way to lose ten minutes on localhost.
if len(settings_mod.PROFILES) == 1:
    profile = settings_mod.PROFILES[0]
else:
    # Start on whichever target this checkout is set up for.
    profile = st.sidebar.radio(
        "Target system",
        settings_mod.PROFILES,
        index=1 if settings_mod.load("hosted").cmf else 0,
        format_func=lambda p: "Local stack (compose)" if p == "local" else "Hosted (Confluent + CMF)",
        help="Local reads localhost:29092 and the session cluster's REST API; hosted reads "
             "the brokers and CMF from your lab.yaml.",
    )
cfg = settings_mod.load(profile)
cfg.apply()

# Which pipeline is on screen is not a preference — it is whichever one the
# student has deployed, and the control plane is asked rather than guessed. See
# inspector/deployment.py; the manual choice stays for looking at a pipeline
# that is not deployed yet.
@st.cache_data(ttl=5.0, show_spinner=False)
def detect_deployment(signature: str) -> deployment_mod.Deployed:
    return deployment_mod.detect(cfg.flink_rest_url, cfg.cmf)


deployed = detect_deployment(f"{profile}|{deployment_mod.student_id()}")

choice = st.sidebar.radio(
    "Pipeline",
    ("auto", "simple", "full"),
    format_func=lambda k: {
        "auto": "Deployed (detected)",
        "simple": f"{deployment_mod.SIMPLE.label} (simple)",
        "full": f"{deployment_mod.FULL.label} (full)",
    }[k],
    help="'Deployed' follows whichever application is actually out there. The other "
         "two draw a pipeline whether or not it is running, which is how you read "
         "the next step before taking it.",
)
if choice == "auto" and deployed.variant is not None:
    variant = deployed.variant
else:
    variant = deployment_mod.BY_KEY[choice if choice != "auto" else "simple"]
job_key = variant.topology

# What CMF is asked about, and it has to agree with the pipeline being drawn:
# the same student has one application name per variant.
_app_names = deployment_mod.candidates()
os.environ["CMF_APPLICATION"] = deployed.app or _app_names.get(variant.key, "")

if deployed.variant is not None:
    icon = "🟢" if deployed.running else "⚪"
    st.sidebar.caption(
        f"{icon} **{deployed.variant.label}** deployed as `{deployed.app}` — "
        f"{deployed.state.lower()}  \n`{deployed.module}`"
    )
elif deployed.source in ("cmf", "flink"):
    st.sidebar.caption(
        f"⚪ Nothing deployed{' for ' + deployment_mod.student_id() if deployment_mod.student_id() else ''}. "
        "Deploy one with `./pipeline.sh deploy simple`."
    )
else:
    st.sidebar.caption("⚪ No control plane reachable — the stage view falls back to topic throughput.")

if deployed.others:
    st.sidebar.warning(
        "Two pipelines are deployed at once (" + ", ".join((deployed.app,) + deployed.others) + "). "
        "That is two TaskManagers for one student — re-run `./pipeline.sh deploy <simple|full>`."
    )

try:
    topo = topology_mod.load(job_key)
except Exception as exc:  # noqa: BLE001 — a bad lab.yaml should explain itself
    st.error(f"Could not read the pipeline configuration: {exc}")
    st.stop()

st.sidebar.divider()
# The one action a student takes on this system, and it is not in this app:
# uploading a document to the bucket. The link is configuration (COS_BUCKET_URL)
# rather than a constant, because every lab rebuild mints a different bucket.
if cfg.event_driven:
    st.sidebar.link_button(
        "📤 Upload a document", cfg.cos_bucket_url, width="stretch", type="primary",
        help=f"IBM Cloud Object Storage{' · ' + cfg.cos_bucket if cfg.cos_bucket else ''} — "
             "dropping a file in this bucket is what starts the pipeline",
    )
elif cfg.docling_url and settings_mod.DEV_MODE:
    st.sidebar.caption(
        "No bucket in the local stack: ingest with `make ingest` "
        "(URLs) or `make ingest-files` (local files)."
    )
if cfg.docling_ui_url:
    st.sidebar.link_button(
        "🧾 Docling instance", cfg.docling_ui_url, width="stretch",
        help="The service that converts and chunks, and writes the chunk topic itself",
    )

st.sidebar.divider()
auto = st.sidebar.toggle("Auto-refresh", value=False)
interval = st.sidebar.slider("Every (s)", 2, 30, 5, disabled=not auto)
if st.sidebar.button("Refresh now", width="stretch"):
    st.rerun()

st.sidebar.divider()
st.sidebar.caption(
    f"**Kafka** `{cfg.bootstrap}`  \n"
    f"**Sink** `{topo.sink_type}`  \n"
    f"**Module** `{topo.module}`"
)

signature = f"{profile}|{cfg.bootstrap}|{cfg.get('KAFKA_SECURITY_PROTOCOL')}"


# ----------------------------------------------------------------- header --
# The lab's name first, then which of its two pipelines is on screen — the
# heading answers "what am I looking at" before it answers "which job". It is
# drawn in the conference's own black treatment (see inspector/brand.py) so a
# screenshot of this page can go straight into the session deck.
#
# Ahead of every probe on purpose: unreachable brokers are the normal state of
# this page for the first ten minutes of a workshop, and a student staring at a
# connection error should still be able to see which lab they are in.
_head_note = (
    f"deployed as {deployed.app}" if deployed.variant is not None and deployed.app
    else "not deployed — this is what it would look like"
)
st.markdown(
    brand.header_html(
        LAB_ID, LAB_TITLE, LAB_SUBTITLE,
        note=f"inspecting {topo.title} ({topo.module}) — {_head_note}",
    ),
    unsafe_allow_html=True,
)
# The four systems the document passes through, in that order. A student who
# has never seen this lab before should be able to name them from the header
# alone, before any number on the page means anything.
st.markdown(brand.rail_html(), unsafe_allow_html=True)

if cfg.get("KAFKA_SECURITY_PROTOCOL", "").startswith("SASL") and not (
    cfg.get("KAFKA_API_KEY") and cfg.get("KAFKA_API_SECRET")
):
    st.error(
        "The brokers use SASL but no API key and secret are set. "
        "Fill in `kafka.api_key` and `kafka.api_secret` in your `lab.yaml`."
    )
    st.stop()


# ------------------------------------------------------------- collecting --
@st.cache_data(ttl=2.0, show_spinner=False)
def collect_topics(signature: str, topics: tuple[str, ...]) -> dict:
    reader = _reader(signature)
    return {t: kafka_probe.topic_stats(reader, t) for t in topics}


@st.cache_data(ttl=2.0, show_spinner=False)
def collect_lag(signature: str, group: str, topic: str, _stats) -> kafka_probe.GroupLag:
    return kafka_probe.group_lag(_group_probe(signature, group), group, topic, _stats)


def collect_job() -> tuple[flink_probe.JobView, flink_probe.CmfClient | None]:
    """The best view of the job the reachable control plane can give.

    Flink's REST API first — it is the only one with per-stage metrics — and
    CMF as the fallback, which is what the hosted system actually offers.
    """
    rest_url = cfg.flink_rest_url
    if rest_url:
        rest = flink_probe.FlinkRest(rest_url)
        try:
            if rest.reachable():
                view = rest.find_job(topo.flink_job_name)
                if view is not None:
                    return view, None
                return flink_probe.JobView(source="flink", name=topo.flink_job_name,
                                           state="NOT_RUNNING"), None
        except Exception as exc:  # noqa: BLE001
            st.session_state["flink_error"] = str(exc)

    cmf_conf = cfg.cmf
    if cmf_conf:
        client = flink_probe.CmfClient(*cmf_conf)
        return client.application(topo.cmf_app), client
    return flink_probe.JobView(source="none", name=topo.flink_job_name, state="UNKNOWN"), None


now = time.time()
with st.spinner("Reading Kafka…"):
    try:
        stats = collect_topics(signature, tuple(topo.topics()))
    except SystemExit as exc:      # labtools.kafka exits on missing credentials
        st.error(f"Kafka connection is not configured: {exc}")
        st.stop()
    except Exception as exc:  # noqa: BLE001
        st.error(f"Cannot reach Kafka at `{cfg.bootstrap}`: {exc}")
        st.stop()

input_stats = stats.get(topo.input_topic)
lag = collect_lag(signature, topo.consumer_group, topo.input_topic, input_stats) if input_stats else None
job, cmf = collect_job()

rates = {t: rate_of(f"{signature}|{t}", s.end_offset, now) for t, s in stats.items()}
# None when the group commits no offsets — "unknown", not "zero". See GroupLag.
backlog = lag.backlog if lag else None
input_rate = rates.get(topo.input_topic)
# Whichever terminal writes a topic, which is not necessarily the first one:
# SINK_TYPE names a list and nothing fixes its order, so `opensearch,kafka`
# puts the topic on the second sink node.
output_topic = next(
    (n.topic for n in topo.nodes if n.kind == topology_mod.SINK and n.topic
     and n.id.startswith("sink")),
    "",
)
output_rate = rates.get(output_topic)


# --------------------------------------------------- stage activity model --
def vertex_for(flink_name: str) -> flink_probe.Vertex | None:
    """Match a stage to a Flink vertex by name.

    Flink chains operators unless the job turns chaining off, so a vertex is
    named for everything fused into it (``Source: … -> prepare``)
    and several stages legitimately match the same one.
    """
    if job.source != "flink" or not flink_name:
        return None
    for vertex in job.vertices:
        if flink_name in vertex.name:
            return vertex
    return None


def node_states() -> dict[str, NodeState]:
    states: dict[str, NodeState] = {}
    running = job.running
    stalled = bool(backlog)   # a real, reported backlog — see the metric above

    for node in topo.nodes:
        # Upstream of Kafka there is nothing to measure from here: the bucket,
        # the event and the Docling job all happen in someone else's cluster,
        # and the first evidence they ran is a message on the chunk topic.
        if node.kind == topology_mod.EXT:
            host = node.url.split("//")[-1].split("/")[0] if node.url else ""
            states[node.id] = NodeState(EXTERNAL, (host,) if host else (), node.why)
            continue

        if not node.enabled:
            states[node.id] = NodeState(OFF, ("(disabled)",), f"{node.why} — turned off in this configuration")
            continue

        # Topics answer for themselves: a count and a rate, straight from the
        # broker's watermarks.
        if node.topic:
            stat = stats.get(node.topic)
            rate = rates.get(node.topic)
            if stat is None or not stat.exists:
                states[node.id] = NodeState(UNKNOWN, ("topic missing",), "Not created yet — run `make topics`")
                continue
            lines = [f"{stat.total:,} msgs", fmt_rate(rate)]
            if node.id == "in" and backlog:
                lines.append(f"lag {backlog:,}")
            status = FLOWING if (rate or 0) > 0 else (BACKLOG if (node.id == "in" and stalled) else IDLE)
            states[node.id] = NodeState(status, tuple(lines), node.why)
            continue

        # Operators: Flink's own numbers when the JobManager is reachable...
        vertex = vertex_for(node.flink_name)
        if vertex is not None:
            out_rate = rate_of(f"{signature}|v|{vertex.id}", vertex.records_out, now)
            busy = vertex.busy_ratio
            lines = [f"{vertex.records_out:,} out"]
            if busy is not None:
                lines.append(f"busy {busy * 100:.0f}%")
            if (out_rate or 0) > 0:
                lines.append(fmt_rate(out_rate))
            if busy is not None and busy > 0.5:
                status = BUSY
            elif (out_rate or 0) > 0 or (busy or 0) > 0.02:
                status = FLOWING
            elif vertex.status not in ("RUNNING", ""):
                status = UNKNOWN
            else:
                status = BACKLOG if stalled else IDLE
            tooltip = f"{node.why} — Flink vertex “{vertex.name}” ×{vertex.parallelism}"
            states[node.id] = NodeState(status, tuple(lines), tooltip)
            continue

        # ...and, when it is not, what the topics around it imply. CMF does not
        # proxy the Flink REST API, so on the hosted system this is the only
        # signal that crosses the cluster boundary.
        neighbours = [e.dst for e in topo.edges if e.src == node.id] + \
                     [e.src for e in topo.edges if e.dst == node.id]
        near_rate = max(
            (rates.get(topo.by_id[n].topic) or 0.0)
            for n in neighbours
            if n in topo.by_id and topo.by_id[n].topic
        ) if any(n in topo.by_id and topo.by_id[n].topic for n in neighbours) else 0.0
        if not running:
            status, note = UNKNOWN, "the job is not running"
        elif near_rate > 0 or (input_rate or 0) > 0:
            status, note = FLOWING, "inferred from topic throughput"
        elif stalled:
            status, note = BACKLOG, "the input has a backlog and nothing is moving"
        else:
            status, note = IDLE, "no throughput on the surrounding topics"
        states[node.id] = NodeState(status, (), f"{node.why} — {note}")
    return states


states = node_states()


# ------------------------------------------------------------ the numbers --
head = st.columns(4)
state_icon = {"RUNNING": "🟢", "FAILED": "🔴", "RESTARTING": "🟠", "CANCELED": "⚫"}.get(job.state.upper(), "⚪")
head[0].metric("Job state", f"{state_icon} {job.state}", help=f"via {job.source or 'nothing reachable'}")
head[1].metric(
    f"Input · {topo.input_topic}",
    f"{input_stats.total:,}" if input_stats and input_stats.exists else "–",
    fmt_rate(input_rate) if input_rate is not None else None,
    delta_color="normal" if input_rate else "off",
)
head[2].metric(
    "Backlog (consumer lag)",
    f"{backlog:,}" if backlog is not None else "not reported",
    help=f"Group `{topo.consumer_group}` on `{topo.input_topic}`. A Flink Kafka source only "
         "commits offsets on a completed checkpoint; with checkpointing off the group stays "
         "empty and its lag says nothing about progress. Watch the input and output rates instead.",
)
head[3].metric(
    f"Output · {output_topic or topo.sink_type}",
    f"{stats[output_topic].total:,}" if output_topic in stats and stats[output_topic].exists else "–",
    fmt_rate(output_rate) if output_rate is not None else None,
    delta_color="normal" if output_rate else "off",
)

# A 404 from CMF is not an error when nothing is deployed — it is the answer.
# Saying so plainly, with the command that fixes it, is the whole difference
# between a dashboard that teaches and one that alarms.
if deployed.variant is None and deployed.source in ("cmf", "flink"):
    st.info(
        f"No pipeline is deployed{' for ' + deployment_mod.student_id() if deployment_mod.student_id() else ''}. "
        "The topics below are still real — this is what the diagram will look like once you run "
        "`./pipeline.sh deploy simple`."
    )
elif job.error:
    st.error(job.error)
if backlog and not job.running:
    st.warning(
        f"{backlog:,} message(s) are waiting on `{topo.input_topic}` and the job is `{job.state}`. "
        "Nothing will consume them until it is running."
    )

tab_dag, tab_topics, tab_messages, tab_index, tab_ask, tab_config = st.tabs(
    ["Stages", "Topics", "Messages", "OpenSearch", "Ask", "Configuration"]
)


# ------------------------------------------------------------------- DAG ---
with tab_dag:
    # The one thing a student does to this system is upload a file; everything
    # after it is an event. That is worth stating before the stage graph, not
    # after it.
    st.markdown("#### 📥 Upload a document to the bucket — everything after it is automatic")
    steps = st.columns(2)
    steps[0].markdown(brand.img("ibm-cloud", height=26), unsafe_allow_html=True)
    steps[0].markdown(
        "**1 · Upload to the bucket**  \n"
        f"`{cfg.cos_bucket or 'the workshop bucket'}` on IBM Cloud Object Storage. This is "
        "the only manual step: any object written to the bucket raises an event that "
        "triggers a Docling job. Nothing polls, and nothing is scheduled."
    )
    if cfg.cos_bucket_url:
        steps[0].link_button("Open the bucket ↗", cfg.cos_bucket_url, width="stretch",
                             type="primary")
    steps[1].markdown(brand.img("docling", height=26), unsafe_allow_html=True)
    steps[1].markdown(
        "**2 · Docling converts and chunks**  \n"
        "Conversion *and* chunking happen in Docling, not in Flink. Its "
        "`kafka_chunks` target produces one Kafka message per chunk onto "
        f"`{topo.input_topic}` — which is where this dashboard's numbers start."
    )
    if cfg.docling_ui_url:
        steps[1].link_button("Docling instance ↗", cfg.docling_ui_url, width="stretch")
    st.caption(
        "Neither can be probed from a laptop — the bucket, the event and the job all run "
        "elsewhere. The first evidence that they ran is the message count on "
        f"`{topo.input_topic}`: upload a file, watch that number move."
    )

    if cfg.cos_error:
        st.warning(
            f"`COS_BUCKET_CRN` is set but could not be read ({cfg.cos_error}), so there is "
            "no bucket link. It should look like "
            "`crn:v1:bluemix:public:cloud-object-storage:global:a/<account>:<instance>:"
            "bucket:<name>` — the console's copy button gives it whole.",
            icon="⚠️",
        )
    elif not cfg.event_driven:
        st.info(
            "No bucket is configured, so both stages above are greyed out and there is no "
            "upload link. Copy your bucket's CRN from the IBM Cloud console — there is a "
            "copy button on its Configuration tab — into `student.bucket_crn` in your "
            "`lab.yaml`, then restart this dashboard.",
            icon="ℹ️",
        )
    st.divider()

    left, right = st.columns([4, 1])
    with right:
        show_side = st.toggle("Side outputs", value=True, help="Quarantine, rejects, audit and stats topics")
        show_ingest = st.toggle("Ingest path", value=True,
                                help="The bucket, the event and Docling — everything in front "
                                     "of the chunk topic. Hide it for more room for the job.")
        top_down = st.toggle("Top-down", value=topo.key == "full",
                             help="The v2 graph is two dozen stages: it fits a laptop screen "
                                  "vertically, not horizontally.")
        st.caption(render.legend_markdown().replace(" · ", "  \n"))
    with left:
        st.graphviz_chart(
            render.dot(topo, states, show_side=show_side, show_ingest=show_ingest,
                       rankdir="TB" if top_down else "LR"),
            width="content",
        )

    if job.source == "flink" and job.vertices:
        st.caption("Per-vertex counters from the JobManager. `busy` is the maximum over subtasks — "
                   "one saturated subtask is what makes a stage the bottleneck.")
        st.dataframe(
            [
                {
                    "vertex": v.name,
                    "parallelism": v.parallelism,
                    "status": v.status,
                    "records in": v.records_in,
                    "records out": v.records_out,
                    "busy %": None if v.busy_ratio is None else round(v.busy_ratio * 100, 1),
                    "backpressured %": None if v.backpressure_ms_per_s is None
                    else round(v.backpressure_ms_per_s / 10, 1),
                }
                for v in job.vertices
            ],
            width="stretch",
            hide_index=True,
        )
    else:
        st.info(
            "No JobManager REST API on this target, so stage activity is inferred from the "
            "throughput of the topics around each stage. CMF does not proxy Flink's REST API; "
            "`kubectl port-forward` to a JobManager and set `FLINK_REST_URL` for the per-vertex view.",
            icon="ℹ️",
        )

    with st.expander("What each stage is for"):
        st.dataframe(
            [
                {
                    "stage": n.label,
                    "state": render.STATE_LABEL[states[n.id].status if n.enabled else OFF],
                    "topic": n.topic or "",
                    "why it exists": n.why,
                }
                for n in topo.nodes
                if n.kind != topology_mod.TOPIC or n.topic
            ],
            width="stretch",
            hide_index=True,
        )
    for note in topo.notes:
        st.caption(f"· {note}")


# ---------------------------------------------------------------- topics ---
with tab_topics:
    st.markdown(
        brand.inline("confluent", f"Topics on {cfg.bootstrap} — watermarks read straight "
                                  "from the brokers, no consumer group joined"),
        unsafe_allow_html=True,
    )
    role_of = {n.topic: ("input" if n.id in ("in",) else "control" if n.group == "control"
                         else "output" if n.id == "sink" else "side output")
               for n in topo.nodes if n.topic}
    rows = []
    for topic, stat in stats.items():
        rows.append(
            {
                "topic": topic,
                "role": role_of.get(topic, ""),
                "messages": stat.total if stat.exists else None,
                "rate": fmt_rate(rates.get(topic)),
                "partitions": stat.partitions if stat.exists else None,
                "status": "ok" if stat.exists else "not created",
            }
        )
    st.dataframe(rows, width="stretch", hide_index=True)

    st.subheader("Consumer groups")
    groups = {topo.consumer_group: topo.input_topic}
    if job_key == "full":
        groups[f"{topo.consumer_group}-policy"] = topo.by_id["policy"].topic
    lag_rows = []
    for group, topic in groups.items():
        stat = stats.get(topic)
        if stat is None:
            continue
        gl = collect_lag(signature, group, topic, stat)
        lag_rows.append(
            {
                "group": group,
                "topic": topic,
                "committed": gl.committed if gl.tracked else None,
                "end offset": gl.end_offset,
                "lag": gl.backlog,
                "note": gl.error or ("" if gl.tracked else "group commits no offsets (checkpointing off)"),
            }
        )
    st.dataframe(lag_rows, width="stretch", hide_index=True)
    st.caption(
        "Lag would be the honest answer to “is this pipeline keeping up?” — messages produced "
        "minus messages the job committed — but a Flink Kafka source only commits on a completed "
        "checkpoint, and these jobs run without checkpointing. When the note says so, the lag "
        "column is blank and throughput is the signal to watch. Read without joining the group, "
        "so looking never disturbs the job."
    )

    if job.source == "cmf" and cmf is not None:
        with st.expander("CMF application events"):
            events = cmf.events(topo.cmf_app)
            if events:
                st.dataframe(
                    [
                        {
                            "time": e.get("metadata", {}).get("creationTimestamp") or e.get("lastTimestamp", ""),
                            "type": e.get("type", ""),
                            "reason": e.get("reason", ""),
                            "message": e.get("message", ""),
                        }
                        for e in events
                    ],
                    width="stretch",
                    hide_index=True,
                )
            else:
                st.caption("No events returned.")


# -------------------------------------------------------------- messages ---
with tab_messages:
    try:
        all_topics = sorted(kafka_probe.cluster_topics(_admin(signature)))
    except Exception:  # noqa: BLE001
        all_topics = []
    known = [t for t in topo.topics() if t in stats]
    choices = known + [t for t in all_topics if t not in known]

    controls = st.columns([3, 1, 1, 2])
    topic = controls[0].selectbox("Topic", choices, index=0 if choices else None)
    count = controls[1].number_input("Messages", 1, 200, 20, step=5)
    newest = controls[2].selectbox("From", ("newest", "oldest")) == "newest"
    needle = controls[3].text_input("Filter (substring)", "")

    if topic:
        with st.spinner(f"Reading {topic}…"):
            records = kafka_probe.read_topic(_reader(signature), topic, int(count), newest_first=newest)
        if needle:
            records = [r for r in records if needle.lower() in r["value"].lower()]
        st.caption(f"{len(records)} message(s) from `{topic}` — read without committing any offset.")

        if records:
            st.dataframe(
                [
                    {"p": r["partition"], "offset": r["offset"], **render.record_row(r["value"])}
                    for r in records
                ],
                width="stretch",
                hide_index=True,
            )
            st.divider()
            for record in records:
                summary = render.summarize_record(record["value"]).splitlines()[0]
                label = f"p{record['partition']}@{record['offset']} · {summary}"
                with st.expander(label[:160]):
                    st.json(render.record_fields(record["value"]))
        else:
            st.info(
                "Nothing on this topic yet. Upload a document to your bucket — the button "
                "is in the sidebar."
                if not settings_mod.DEV_MODE else
                "Nothing on this topic yet. Ingest something with `make ingest` / `make remote-ingest`."
            )


# ------------------------------------------------------------ opensearch ---
with tab_index:
    hosts = cfg.opensearch_hosts
    index_name = cfg.get("OPENSEARCH_INDEX", "document-chunks")
    st.markdown(
        brand.inline("opensearch", f"Index {index_name} on {hosts}" if hosts
                     else f"Index {index_name} — no host configured"),
        unsafe_allow_html=True,
    )
    client = opensearch_client()
    view = opensearch_probe.index_view(client, index_name)

    if not view.reachable:
        # "Not reachable" has three causes that look identical and want three
        # different fixes — a refused password, an untrusted certificate, an
        # unreachable host. Saying which is most of the value of this panel
        # during the first ten minutes of a class.
        why = opensearch_probe.why_unreachable(
            hosts, cfg.get("OPENSEARCH_USERNAME"), cfg.get("OPENSEARCH_PASSWORD"),
            cfg.opensearch_verify, cfg.opensearch_ca,
        )
        st.info(
            f"No OpenSearch at `{hosts or '(not configured)'}`."
            + (f"\n\n{why}" if why else "")
            + f"\n\nThe sink is `{topo.sink_type}`"
            + (", so the finished records are also on a topic — look at them in the Messages tab."
               if "kafka" in topo.sink_type else "."),
            icon="ℹ️",
        )
    elif not view.exists:
        st.warning(f"Connected, but the index `{index_name}` does not exist yet "
                   "(create it with `python scripts/setup_opensearch.py`).")
        if view.indices:
            st.caption("Indices present: " + ", ".join(view.indices))
    else:
        cols = st.columns(3)
        cols[0].metric("Documents", f"{view.doc_count:,}")
        cols[1].metric("Store size", f"{view.size_bytes / 1e6:.1f} MB")
        cols[2].metric("Vector dimension", view.dimension or "–")
        query = st.text_input("Search (text, headings)", "", placeholder="leave empty for the newest documents")
        hits = opensearch_probe.recent(client, index_name, size=25, query=query)
        if hits:
            st.dataframe(
                [
                    {
                        # match_all is sorted by ingest time and carries no score.
                        **({"score": round(h.get("_score") or 0, 3)} if query else {}),
                        "doc_id": (h.get("doc_id") or "")[:16],
                        "chunk": h.get("chunk_index"),
                        "chars": h.get("char_count"),
                        "heading": (h.get("headings") or [""])[-1][:40] if h.get("headings") else "",
                        "text": (h.get("text") or "")[:120].replace("\n", " "),
                    }
                    for h in hits
                ],
                width="stretch",
                hide_index=True,
            )
            for hit in hits[:10]:
                with st.expander(f"{hit['_id'][:24]} · {(hit.get('text') or '')[:80]}"):
                    st.json(hit)
        else:
            st.caption("No documents matched.")
        if view.error:
            st.error(view.error)


# --------------------------------------------------------------------- ask --
# The end of the lab, and the only tab that asks the system a question instead
# of reporting on it. Everything it uses was built by the pipeline: the chunks
# are Docling's, the vectors are the embed stage's, the index is the sink's.
# What is left — embed the question, search, prompt, answer — is four calls, and
# that is the point worth making after four phases of building the thing that
# made them possible.
with tab_ask:
    index_name = cfg.get("OPENSEARCH_INDEX", "document-chunks")
    st.markdown(
        brand.inline("opensearch", f"Ask {index_name} — retrieval-augmented generation"),
        unsafe_allow_html=True,
    )
    st.caption(
        "Your question is embedded by the same model that embedded the chunks "
        f"(`{cfg.get('EMBEDDING_MODEL_ID', '-')}`), the nearest chunks are pulled out of the "
        "index by k-NN, and a watsonx.ai chat model is asked to answer **out of those chunks "
        "and nothing else**. Every passage it was given is shown below the answer, so you can "
        "check the citations against the text."
    )

    ask_client = opensearch_client()
    ask_view = opensearch_probe.index_view(ask_client, index_name)

    # Three preconditions, each with its own fix. Checked in the order a student
    # meets them, and each stops here rather than letting the next one produce a
    # more confusing error.
    if not ask_view.reachable:
        st.info(
            f"No OpenSearch at `{cfg.opensearch_hosts or '(not configured)'}` — there is nothing "
            "to retrieve from yet. The OpenSearch tab says why.",
            icon="ℹ️",
        )
    elif not ask_view.exists:
        st.warning(
            f"The index `{index_name}` does not exist yet. Create it with "
            "`./setup.sh index`, then upload a document to your bucket."
        )
    elif not ask_view.doc_count:
        st.warning(
            f"`{index_name}` exists but is empty. Upload a document to your bucket and wait for "
            "the pipeline to index it — the OpenSearch tab's document count is the thing to watch."
        )
    else:
        if not cfg.watsonx_ready:
            st.error(
                "The retrieval half works, but answering needs watsonx.ai: set "
                "`watsonx.project_id` in your `lab.yaml`, and `WATSONX_APIKEY` in your "
                "shell — the same two the pipeline's embed stage uses."
            )

        with st.form("ask"):
            question = st.text_input(
                "Question",
                placeholder="What does this document say about …?",
                help="Answered only from the chunks retrieved out of your index.",
            )
            controls = st.columns([1, 1, 2])
            top_k = controls[0].slider(
                "Passages", 1, 15, 6,
                help="How many chunks to retrieve and hand to the model. More context is not "
                     "automatically better: the answer has to be found in what you pass, and "
                     "a large k buries it.",
            )
            lexical = controls[1].toggle(
                "Keyword boost", value=True,
                help="Adds the question's own words back as a text match alongside the vector "
                     "search. k-NN alone is weak at exact terms — a part number or a name that "
                     "embeds into nothing in particular.",
            )
            model_id = controls[2].selectbox(
                "Answer model",
                ("meta-llama/llama-3-3-70b-instruct", "mistralai/mistral-small-3-1-24b-instruct-2503"),
                help="ca-tor serves exactly these two chat models. The catalogue is regional — "
                     "asking for a third is a 404, not a fallback.",
            )
            asked = st.form_submit_button("Ask", type="primary", disabled=not cfg.watsonx_ready)

        if asked and question.strip():
            with st.spinner("Embedding the question, searching, answering…"):
                result = rag_mod.answer(
                    ask_client, index_name, question,
                    k=top_k, lexical=lexical, dimension=ask_view.dimension,
                    generation=GenerationConfig(model_id=model_id),
                )

            if result.error:
                st.error(result.error)
            if result.ok:
                st.markdown("#### Answer")
                st.markdown(result.text)
                st.caption(
                    f"`{result.model_id}` · answered from {len(result.cited)} of "
                    f"{len(result.passages)} retrieved passages"
                )

            if result.passages:
                st.divider()
                st.markdown("#### Retrieved passages")
                st.caption(
                    "In the order the search returned them, numbered as the answer cites them. "
                    "A cited passage is expanded — the claim and the text it came from should "
                    "be checkable side by side, including when they disagree."
                )
                for passage in result.passages:
                    used = passage.n in result.cited
                    label = f"{'✅' if used else '·'} [{passage.n}] {passage.where} — score {passage.score:.3f}"
                    with st.expander(label, expanded=used):
                        st.write(passage.text)
                        st.caption(f"`{passage.chunk_id}`")


# ----------------------------------------------------------- configuration --
with tab_config:
    st.caption(
        "What this dashboard resolved, and where it looked: your `lab.yaml`, with any "
        "variable already set in your shell taking precedence. Secrets are masked. "
        "This is the first place to look when something is pointed at the wrong thing."
    )
    st.dataframe(
        [
            {"variable": k, "value": "••••••" if k in SECRET_KEYS and v else v}
            for k, v in sorted(cfg.values.items())
        ],
        width="stretch",
        hide_index=True,
    )
    st.subheader("This pipeline")
    # The bucket is in here rather than only in the table above because it is the
    # one name in the lab that nothing can check: a wrong topic shows up as an
    # empty count, a wrong bucket shows up as an upload that silently goes to
    # somebody else's pipeline.
    st.code(
        f"module          {topo.module}\n"
        f"flink job name  {topo.flink_job_name}\n"
        f"cmf application {topo.cmf_app}\n"
        f"input topic     {topo.input_topic}\n"
        f"consumer group  {topo.consumer_group}\n"
        f"sink            {topo.sink_type}\n"
        f"topics          {', '.join(topo.topics())}\n"
        f"lab environment {cfg.lab_env or '-'}\n"
        f"cos bucket      {cfg.cos_bucket or '-'}",
        language="text",
    )

if auto:
    time.sleep(interval)
    st.rerun()
