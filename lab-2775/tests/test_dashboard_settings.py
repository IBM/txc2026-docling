"""The dashboard's bucket link, per student.

``./pipeline.sh inspect`` — and the instructor's fan-out, once per student — point the
dashboard at one student by exporting their names into the environment, and the
bucket is the awkward one: its CRN may be unknown for that student even though
the config file holds someone else's. Two rules come out of that, and both have
been broken once:

* an exported *empty* ``COS_BUCKET_CRN`` must not fall back to the file, or
  every student's upload button opens the instructor's bucket;
* it must still produce a link, because the console addresses the *instance*
  in the URL path and names the bucket in the query string — so any CRN from
  the same instance is enough, and the name comes from the student's own
  environment.
"""

from __future__ import annotations

import pytest

settings = pytest.importorskip("inspector.settings")

INSTANCE = "a/793a70a62275449e8baa35262f4e4d3c:e8db6702-e181-48df-8cf2-df4a55c59448"
FILE_CRN = f"crn:v1:bluemix:public:cloud-object-storage:global:{INSTANCE}:bucket:txc26-ws-02-uploads"

# What a developer's config holds: their own bucket, and nothing per student.
DOTENV = {
    "KAFKA_BOOTSTRAP_SERVERS": "broker:9094",
    "COS_BUCKET_CRN": FILE_CRN,
    "COS_BUCKET_REGION": "ca-tor",
    "COS_BUCKET": "",
    "COS_BUCKET_URL": "",
}


@pytest.fixture
def env(monkeypatch):
    """Load settings against a fixed config file and a controllable shell.

    Both sources are stubbed: what is under test is the layering — file, then
    shell, with the bucket's explicit-empty rule on top — not where the values
    come from. The real ``lab.yaml`` is whatever the developer running the
    tests happens to have, which would otherwise decide the assertions.
    """
    monkeypatch.setattr(settings, "_dotenv", lambda _path: dict(DOTENV))
    monkeypatch.setattr(settings, "_lab_yaml", dict)

    def load(profile: str = "hosted", **shell: str):
        monkeypatch.setattr(settings, "_SHELL_ENV", dict(shell))
        # The local profile only exists in a development checkout (LAB_DEV=1),
        # and asking for it must still work when a test does so deliberately.
        monkeypatch.setattr(settings, "PROFILES", ("local", "hosted"))
        return settings.load(profile)

    return load


def test_the_env_file_alone_links_to_its_own_bucket(env):
    """`make inspect`, with nothing exported — the behaviour that always worked."""
    cfg = env()
    assert cfg.cos_bucket == "txc26-ws-02-uploads"
    assert cfg.event_driven
    assert "bucket=txc26-ws-02-uploads" in cfg.cos_bucket_url


def test_a_students_own_crn_wins(env):
    crn = FILE_CRN.replace("txc26-ws-02-uploads", "txc26-ws-07-uploads-md")
    cfg = env(COS_BUCKET_CRN=crn, COS_BUCKET="txc26-ws-07-uploads-md")
    assert cfg.cos_bucket == "txc26-ws-07-uploads-md"
    assert "bucket=txc26-ws-07-uploads-md" in cfg.cos_bucket_url


def test_a_cleared_crn_still_links_but_to_the_students_bucket(env):
    """The instructor's fan-out: no CRN for student 07, but a link all the same.

    The instance comes from the file; the bucket name must not.
    """
    cfg = env(COS_BUCKET_CRN="", COS_BUCKET="txc26-ws-07-uploads", COS_BUCKET_URL="")
    assert cfg.cos_bucket == "txc26-ws-07-uploads"
    assert cfg.event_driven, "the upload button has to be there"
    assert "bucket=txc26-ws-07-uploads" in cfg.cos_bucket_url
    assert "txc26-ws-02-uploads" not in cfg.cos_bucket_url
    # The instance is still the file's — it is the same COS instance.
    assert "e8db6702-e181-48df-8cf2-df4a55c59448" in cfg.cos_bucket_url


def test_a_cleared_crn_and_no_bucket_name_gives_no_link(env):
    """Nothing known about this student's bucket: no link beats a wrong one."""
    cfg = env(COS_BUCKET_CRN="", COS_BUCKET="", COS_BUCKET_URL="")
    assert cfg.cos_bucket == ""
    assert not cfg.event_driven


def test_an_explicit_instance_crn_is_preferred_over_the_file(env):
    other = "a/999:aaaa-bbbb"
    cfg = env(
        COS_BUCKET_CRN="",
        COS_BUCKET="txc26-ws-07-uploads",
        COS_BUCKET_URL="",
        COS_INSTANCE_CRN=f"crn:v1:bluemix:public:cloud-object-storage:global:{other}::",
    )
    assert "aaaa-bbbb" in cfg.cos_bucket_url
    assert "e8db6702" not in cfg.cos_bucket_url


def test_the_local_profile_has_no_bucket_at_all(env):
    """The config must not leak a bucket into the compose stack, which has none."""
    cfg = env("local")
    assert cfg.cos_bucket == ""
    assert not cfg.event_driven


def test_the_local_profile_is_not_offered_to_a_student(monkeypatch):
    """It is a compose stack that is not published with the lab, and a
    student has no way to start it — so a "Target system" choice with one
    working answer is only a way to lose ten minutes on localhost."""
    import importlib

    monkeypatch.delenv("LAB_DEV", raising=False)
    assert importlib.reload(settings).PROFILES == ("hosted",)
    monkeypatch.setenv("LAB_DEV", "1")
    assert importlib.reload(settings).PROFILES == ("local", "hosted")
    monkeypatch.delenv("LAB_DEV", raising=False)
    importlib.reload(settings)


def test_a_malformed_crn_is_reported_not_raised(env):
    cfg = env(COS_BUCKET_CRN="not-a-crn")
    assert cfg.cos_error
    assert not cfg.event_driven
