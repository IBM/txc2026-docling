"""Where the job's own view of itself comes from — and what is missing.

There are two control planes in this repo and they answer different questions:

* **Flink's REST API** (the local compose stack, ``localhost:8081``) knows the
  job *graph*: one vertex per operator, with records in and out and a
  ``busyTimeMsPerSecond`` per subtask. This is the only source that can say
  which stage is busy right now.
* **CMF** (the hosted system) knows the *application*: state, job id, cluster
  size, checkpoints and lifecycle events. Its Flink REST is inside the
  Kubernetes cluster and CMF does not proxy it, so vertex-level metrics are not
  available there — the dashboard falls back to Kafka throughput, which crosses
  the cluster boundary because the topics do.

Set ``FLINK_REST_URL`` if you do have a route to a JobManager (a
``kubectl port-forward``, say) and the hosted view gains the vertex detail too.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import requests
import urllib3

# The lab's CMF presents a self-signed certificate — every curl in
# scripts/cmf.sh carries -k for the same reason — so the warning would fire on
# every poll and say nothing new.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TIMEOUT_S = 6.0


@dataclass
class Vertex:
    id: str
    name: str
    parallelism: int = 0
    status: str = ""
    records_in: int = 0
    records_out: int = 0
    busy_ms_per_s: float | None = None      # None = Flink reports N/A (sources/sinks)
    backpressure_ms_per_s: float | None = None

    @property
    def busy_ratio(self) -> float | None:
        return None if self.busy_ms_per_s is None else min(1.0, self.busy_ms_per_s / 1000.0)


@dataclass
class JobView:
    """One running job, as much as the reachable control plane will say."""

    source: str                              # "flink" | "cmf" | "none"
    name: str = ""
    job_id: str = ""
    state: str = "UNKNOWN"
    start_time: int = 0
    vertices: list[Vertex] = field(default_factory=list)
    detail: dict = field(default_factory=dict)
    error: str = ""

    @property
    def running(self) -> bool:
        return self.state.upper() == "RUNNING"


# --------------------------------------------------------------- Flink REST --
class FlinkRest:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def _get(self, path: str, **params):
        r = requests.get(f"{self.base_url}{path}", params=params or None, timeout=TIMEOUT_S)
        r.raise_for_status()
        return r.json()

    def reachable(self) -> bool:
        try:
            self._get("/config")
            return True
        except Exception:  # noqa: BLE001 — any failure means "no route", not a bug
            return False

    def jobs(self) -> list[dict]:
        return self._get("/jobs/overview").get("jobs", [])

    def job(self, job_id: str, with_busy: bool = True) -> JobView:
        payload = self._get(f"/jobs/{job_id}")
        view = JobView(
            source="flink",
            name=payload.get("name", ""),
            job_id=job_id,
            state=payload.get("state", "UNKNOWN"),
            start_time=payload.get("start-time", 0) or 0,
            detail=payload,
        )
        for raw in payload.get("vertices", []):
            metrics = raw.get("metrics", {})
            vertex = Vertex(
                id=raw.get("id", ""),
                name=raw.get("name", ""),
                parallelism=raw.get("parallelism", 0),
                status=raw.get("status", ""),
                records_in=metrics.get("read-records", 0) or 0,
                records_out=metrics.get("write-records", 0) or 0,
            )
            if with_busy and view.running:
                vertex.busy_ms_per_s, vertex.backpressure_ms_per_s = self._busy(job_id, vertex.id)
            view.vertices.append(vertex)
        return view

    def _busy(self, job_id: str, vertex_id: str) -> tuple[float | None, float | None]:
        """Per-subtask busy/backpressure, aggregated to the max over subtasks.

        ``max`` rather than an average on purpose: one saturated subtask is
        what makes a stage the bottleneck, and averaging it away is exactly
        what hides that.
        """
        try:
            rows = self._get(
                f"/jobs/{job_id}/vertices/{vertex_id}/subtasks/metrics",
                get="busyTimeMsPerSecond,backPressuredTimeMsPerSecond",
                agg="max",
            )
        except Exception:  # noqa: BLE001
            return None, None
        values: dict[str, float | None] = {}
        for row in rows:
            raw = row.get("max")
            try:
                values[row.get("id", "")] = None if raw in (None, "NaN") else float(raw)
            except (TypeError, ValueError):
                values[row.get("id", "")] = None
        return values.get("busyTimeMsPerSecond"), values.get("backPressuredTimeMsPerSecond")

    def find_job(self, name: str) -> JobView | None:
        """The newest job whose name matches, running ones first."""
        candidates = [j for j in self.jobs() if j.get("name") == name]
        if not candidates:
            return None
        candidates.sort(key=lambda j: (j.get("state") == "RUNNING", j.get("start-time", 0)), reverse=True)
        return self.job(candidates[0]["jid"])


# ---------------------------------------------------------------------- CMF --
class CmfClient:
    """The subset of CMF's API the dashboard needs — read-only.

    Same endpoints ``scripts/cmf.sh status`` calls, in Python and without the
    deploy half: a dashboard a student runs should not be able to replace a
    running application by accident.
    """

    def __init__(self, base_url: str, environment: str, auth: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.environment = environment
        user, _, password = auth.partition(":")
        self.auth = (user, password) if user else None

    @property
    def _api(self) -> str:
        return f"{self.base_url}/api/v1/environments/{self.environment}/applications"

    def _get(self, path: str = ""):
        # verify=False: the lab's CMF presents a self-signed certificate, which
        # is why every curl in scripts/cmf.sh carries -k.
        r = requests.get(f"{self._api}{path}", auth=self.auth, timeout=TIMEOUT_S, verify=False)
        r.raise_for_status()
        return r.json()

    def reachable(self) -> bool:
        try:
            self._get()
            return True
        except Exception:  # noqa: BLE001
            return False

    def applications(self) -> list[dict]:
        return self._get().get("items", [])

    def application(self, name: str) -> JobView:
        try:
            payload = self._get(f"/{name}")
        except Exception as exc:  # noqa: BLE001
            return JobView(source="cmf", name=name, state="NOT_DEPLOYED", error=str(exc))
        status = payload.get("status") or {}
        job = status.get("jobStatus") or {}
        return JobView(
            source="cmf",
            name=job.get("jobName") or name,
            job_id=job.get("jobId") or "",
            state=job.get("state") or status.get("lifecycleState") or "UNKNOWN",
            start_time=int(job.get("startTime") or 0),
            detail=payload,
            error=status.get("error") or "",
        )

    def events(self, name: str, limit: int = 20) -> list[dict]:
        try:
            payload = self._get(f"/{name}/events")
        except Exception:  # noqa: BLE001
            return []
        items = payload.get("items", payload) if isinstance(payload, dict) else payload
        return list(items)[-limit:][::-1] if isinstance(items, list) else []
