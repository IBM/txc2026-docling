"""Which of the two pipelines is deployed right now — asked, not assumed.

A student runs one pipeline at a time and switches between them with
``./pipeline.sh deploy simple|full``, which deletes the application that is
there before creating the other one. So "which am I looking at" is a property of
the cluster, not of a radio button, and getting it wrong is the kind of mistake
a teaching dashboard must not make: every stage on the diagram, every topic
panel and every count would describe a pipeline that is not running.

Two ways to answer it, in the order the control planes are preferred elsewhere:

* **CMF** — one ``GET`` of the application collection carries each application's
  full descriptor, and ``spec.job.args`` holds the ``-pym`` module the job was
  submitted with. That module *is* the answer, and it comes from the thing that
  actually deployed rather than from this dashboard's configuration.
* **Flink REST** — no application objects there, but the two jobs call
  ``env.execute()`` with different names, so a running job identifies itself.

Neither is a guess. When neither answers, the dashboard says nothing is deployed
instead of drawing a pipeline that may not exist.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from . import flink_probe


@dataclass(frozen=True)
class Variant:
    """One of the workshop's two pipelines, under all the names it goes by."""

    key: str          # what a student types: simple | full
    topology: str     # the key inspector.topology draws: enrich | full
    module: str       # the -pym argument, which is what CMF reports
    job_name: str     # the string the job passes to env.execute()
    label: str
    blurb: str
    default_app: str  # the application name outside the workshop fan-out


SIMPLE = Variant(
    key="simple",
    topology="enrich",
    module="pipeline.enrich_job",
    job_name="chunk-enrich-pipeline",
    label="Simple pipeline",
    blurb="prepare → embed → sink",
    default_app="docling-chunk-enrich",
)
FULL = Variant(
    key="full",
    topology="full",
    module="pipeline.full_job",
    job_name="chunk-guard-pipeline",
    label="Full pipeline",
    blurb="…plus the policy broadcast, the PII guard, its audit outputs and dedup",
    default_app="docling-chunk-full",
)
VARIANTS: tuple[Variant, ...] = (SIMPLE, FULL)

BY_KEY = {v.key: v for v in VARIANTS}
BY_TOPOLOGY = {v.topology: v for v in VARIANTS}
BY_MODULE = {v.module: v for v in VARIANTS}


@dataclass(frozen=True)
class Deployed:
    """What the control plane says is out there."""

    variant: Variant | None = None
    app: str = ""
    state: str = ""
    module: str = ""
    source: str = ""                  # cmf | flink | none
    others: tuple[str, ...] = ()      # further applications of the same student
    error: str = ""

    @property
    def found(self) -> bool:
        return self.variant is not None or bool(self.app)

    @property
    def running(self) -> bool:
        return self.state.upper() == "RUNNING"


def student_id() -> str:
    """``WS_ID``, normalised to the two digits every name is built from."""
    raw = (os.environ.get("WS_ID") or "").strip()
    if raw.isdigit():
        return f"{int(raw):02d}"
    return ""


def candidates() -> dict[str, str]:
    """Application name to look for, per variant.

    In the workshop that is ``ws-07-simple`` / ``ws-07-full``; outside it, the
    single application ``CMF_APPLICATION`` names, or the two defaults
    ``scripts/cmf.sh`` deploys.
    """
    ident = student_id()
    if ident:
        prefix = os.environ.get("WS_PREFIX", "ws")
        return {v.key: f"{prefix}-{ident}-{v.key}" for v in VARIANTS}
    explicit = os.environ.get("CMF_APPLICATION", "").strip()
    if explicit:
        # One name and no way to know which variant it holds — the module in its
        # descriptor decides, so both candidates point at it and whichever
        # matches wins.
        return {v.key: explicit for v in VARIANTS}
    return {v.key: v.default_app for v in VARIANTS}


def legacy_app() -> str:
    """``ws-07``, from the fan-out that predates the two variants.

    Nothing creates it any more and ``ws_switch_to`` deletes it on the next
    deploy, but until then it is a running job with a student's name on it, and
    reporting "nothing deployed" over the top of one is the single worst thing
    this dashboard can say.
    """
    ident = student_id()
    return f"{os.environ.get('WS_PREFIX', 'ws')}-{ident}" if ident else ""


def module_of(app: dict) -> str:
    """The ``-pym`` argument of a CMF application descriptor."""
    args = ((app.get("spec") or {}).get("job") or {}).get("args") or []
    args = [str(a) for a in args]
    if "-pym" in args:
        index = args.index("-pym") + 1
        if index < len(args):
            return args[index]
    return ""


def state_of(app: dict) -> str:
    status = app.get("status") or {}
    job = status.get("jobStatus") or {}
    return job.get("state") or status.get("lifecycleState") or "UNKNOWN"


def name_of(app: dict) -> str:
    return (app.get("metadata") or {}).get("name") or ""


def from_cmf(client: flink_probe.CmfClient) -> Deployed:
    """The deployed application among this student's candidates.

    One request: the collection carries every descriptor, so asking for two
    names costs the same as asking for one, and the same response reveals the
    case worth warning about — both pipelines deployed at once, which is two
    TaskManagers held for one student.
    """
    wanted: list[tuple[str, str]] = [(name, key) for key, name in candidates().items()]
    if legacy_app():
        wanted.append((legacy_app(), ""))
    try:
        apps = {name_of(a): a for a in client.applications()}
    except Exception as exc:  # noqa: BLE001 — an unreachable CMF is a normal state here
        return Deployed(source="none", error=str(exc))

    hits: list[tuple[Variant, dict]] = []
    seen: set[str] = set()
    for app_name, key in wanted:
        app = apps.get(app_name)
        if app is None or app_name in seen:
            continue
        seen.add(app_name)
        # The descriptor is the authority: an application called `-full` running
        # the simple module is reported as what it runs, not as what it is named.
        variant = BY_MODULE.get(module_of(app)) or BY_KEY.get(key)
        if variant is None:
            continue
        hits.append((variant, app))

    if not hits:
        return Deployed(source="cmf")

    # Prefer a running one when a delete is still settling.
    hits.sort(key=lambda h: state_of(h[1]).upper() == "RUNNING", reverse=True)
    variant, app = hits[0]
    return Deployed(
        variant=variant,
        app=name_of(app),
        state=state_of(app),
        module=module_of(app),
        source="cmf",
        others=tuple(name_of(a) for _, a in hits[1:]),
    )


def from_flink(rest: flink_probe.FlinkRest) -> Deployed:
    """The local stand-in: no applications, but the job names differ."""
    try:
        jobs = rest.jobs()
    except Exception as exc:  # noqa: BLE001
        return Deployed(source="none", error=str(exc))
    live = [j for j in jobs if str(j.get("status", "")).upper() == "RUNNING"] or jobs
    for job in live:
        variant = next((v for v in VARIANTS if v.job_name in str(job.get("name", ""))), None)
        if variant is not None:
            return Deployed(
                variant=variant,
                app=str(job.get("name", "")),
                state=str(job.get("status", "")),
                module=variant.module,
                source="flink",
            )
    return Deployed(source="flink")


def detect(flink_rest_url: str, cmf: tuple[str, str, str] | None) -> Deployed:
    """Ask whichever control plane is reachable, CMF first.

    CMF before Flink REST, the opposite of :func:`app.collect_job`'s preference,
    and for a different question: per-stage metrics only exist in Flink's REST
    API, but *which application is deployed* is a thing only CMF knows — the
    hosted JobManager is inside the cluster and is usually not reachable at all.
    """
    if cmf:
        found = from_cmf(flink_probe.CmfClient(*cmf))
        if found.found or not flink_rest_url:
            return found
    if flink_rest_url:
        rest = flink_probe.FlinkRest(flink_rest_url)
        try:
            if rest.reachable():
                return from_flink(rest)
        except Exception as exc:  # noqa: BLE001
            return Deployed(source="none", error=str(exc))
    return Deployed(source="none")
