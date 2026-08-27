"""Tests for the dashboard's "which pipeline is deployed" probe.

``deployment.py`` reaches the network only through the client object it is
handed, which is what lets these tests hand it a recorded CMF response instead.

The skip below is not optional. ``deployment`` imports ``flink_probe``, which
imports ``requests`` at module scope — so in an environment that installed only
one dependency group (the trigger image's, say), this module fails at
*collection*, which takes the whole run down with it rather than skipping one
file.
"""

import pytest

try:
    from inspector import deployment
except ImportError as exc:  # pragma: no cover - depends on what is installed
    pytest.skip(f"dashboard dependencies not installed ({exc})", allow_module_level=True)


def app(name: str, module: str, state: str = "RUNNING") -> dict:
    """A CMF application descriptor, cut down to the parts that are read."""
    return {
        "metadata": {"name": name},
        "spec": {"job": {"args": ["-pyclientexec", "/opt/python3", "-pym", module]}},
        "status": {"jobStatus": {"state": state}},
    }


class FakeCmf:
    def __init__(self, *apps: dict) -> None:
        self._apps = list(apps)

    def applications(self) -> list[dict]:
        return self._apps


class FakeRest:
    def __init__(self, *jobs: dict) -> None:
        self._jobs = list(jobs)

    def reachable(self) -> bool:
        return True

    def jobs(self) -> list[dict]:
        return self._jobs


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for key in ("WS_ID", "WS_PREFIX", "CMF_APPLICATION"):
        monkeypatch.delenv(key, raising=False)


# --- names ------------------------------------------------------------------
def test_student_id_is_normalised_to_two_digits(monkeypatch):
    monkeypatch.setenv("WS_ID", "7")
    assert deployment.student_id() == "07"
    assert deployment.candidates() == {"simple": "ws-07-simple", "full": "ws-07-full"}
    assert deployment.legacy_app() == "ws-07"


def test_without_a_student_id_the_defaults_are_the_shared_applications():
    assert deployment.candidates()["simple"] == deployment.SIMPLE.default_app
    assert deployment.legacy_app() == ""


def test_an_explicit_application_name_wins(monkeypatch):
    monkeypatch.setenv("CMF_APPLICATION", "my-app")
    # Both candidates point at the one name; its module decides the variant.
    assert set(deployment.candidates().values()) == {"my-app"}


# --- reading a descriptor ---------------------------------------------------
def test_module_and_state_are_read_from_the_descriptor():
    a = app("ws-07-full", "pipeline.full_job", "RESTARTING")
    assert deployment.module_of(a) == "pipeline.full_job"
    assert deployment.state_of(a) == "RESTARTING"
    assert deployment.name_of(a) == "ws-07-full"


def test_a_descriptor_without_pym_yields_no_module():
    assert deployment.module_of({"spec": {"job": {"args": ["-py", "job.py"]}}}) == ""
    assert deployment.module_of({}) == ""


def test_state_falls_back_to_the_lifecycle_when_there_is_no_job_yet():
    assert deployment.state_of({"status": {"lifecycleState": "DEPLOYING"}}) == "DEPLOYING"


# --- detection --------------------------------------------------------------
def test_the_deployed_variant_is_found_by_name(monkeypatch):
    monkeypatch.setenv("WS_ID", "07")
    found = deployment.from_cmf(FakeCmf(app("ws-07-full", "pipeline.full_job")))
    assert found.variant is deployment.FULL
    assert (found.app, found.state, found.running) == ("ws-07-full", "RUNNING", True)
    assert found.others == ()


def test_another_students_application_is_not_mine(monkeypatch):
    monkeypatch.setenv("WS_ID", "07")
    found = deployment.from_cmf(FakeCmf(app("ws-08-full", "pipeline.full_job")))
    assert found.variant is None
    assert found.source == "cmf"


def test_the_module_beats_the_name(monkeypatch):
    """An application named for one variant but submitted with the other module
    is reported as what it runs — the descriptor is the authority."""
    monkeypatch.setenv("WS_ID", "07")
    found = deployment.from_cmf(FakeCmf(app("ws-07-full", "pipeline.enrich_job")))
    assert found.variant is deployment.SIMPLE
    assert found.app == "ws-07-full"


def test_the_pre_variant_application_is_still_recognised(monkeypatch):
    """`ws-07` predates the two variants. Nothing creates it any more, but until
    the next deploy sweeps it away it is a running job with the student's name
    on it, and "nothing deployed" would be a lie."""
    monkeypatch.setenv("WS_ID", "07")
    found = deployment.from_cmf(FakeCmf(app("ws-07", "pipeline.enrich_job")))
    assert found.variant is deployment.SIMPLE
    assert found.app == "ws-07"


def test_two_deployed_at_once_are_reported_running_first(monkeypatch):
    monkeypatch.setenv("WS_ID", "07")
    found = deployment.from_cmf(
        FakeCmf(
            app("ws-07-simple", "pipeline.enrich_job", "FINISHED"),
            app("ws-07-full", "pipeline.full_job", "RUNNING"),
        )
    )
    assert found.variant is deployment.FULL
    assert found.others == ("ws-07-simple",)


def test_an_unreachable_cmf_is_a_state_not_a_crash(monkeypatch):
    monkeypatch.setenv("WS_ID", "07")

    class Broken:
        def applications(self):
            raise RuntimeError("connection refused")

    found = deployment.from_cmf(Broken())
    assert found.variant is None and found.source == "none"
    assert "connection refused" in found.error


def test_the_local_stack_identifies_the_job_by_its_execute_name():
    found = deployment.from_flink(
        FakeRest({"name": "chunk-guard-pipeline", "status": "RUNNING"})
    )
    assert found.variant is deployment.FULL
    assert found.source == "flink"


def test_a_job_that_is_neither_pipeline_is_not_claimed():
    found = deployment.from_flink(FakeRest({"name": "someone-elses-job", "status": "RUNNING"}))
    assert found.variant is None


def test_detect_falls_back_to_flink_when_cmf_knows_nothing(monkeypatch):
    monkeypatch.setattr(deployment.flink_probe, "CmfClient", lambda *a: FakeCmf())
    monkeypatch.setattr(
        deployment.flink_probe, "FlinkRest",
        lambda url: FakeRest({"name": "chunk-enrich-pipeline", "status": "RUNNING"}),
    )
    found = deployment.detect("http://localhost:8081", ("https://cmf", "env", "u:p"))
    assert found.variant is deployment.SIMPLE
    assert found.source == "flink"


def test_detect_without_a_control_plane_says_so():
    assert deployment.detect("", None).source == "none"
