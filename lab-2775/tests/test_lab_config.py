"""``lab.yaml`` — what a student edits — and the environment it becomes.

The cases here are the ones whose failure is *silent*: a name that is derived
wrongly still produces a job that starts, connects and reports itself healthy,
and then reads a topic nobody writes. So the derivations are pinned, and so is
the one thing ``envsubst`` used to get wrong.
"""

from __future__ import annotations

import textwrap

import pytest

from labtools import config as labconfig

MINIMAL = """
student:
  id: "7"
  bucket_crn: ""
  opensearch_password: "hunter2"
kafka:
  bootstrap_servers: "broker:9094"
  api_key: "k"
  api_secret: "s"
  ca_file: ./certs/root-ca.pem
cmf:
  url: "https://cmf/cmf"
  environment: flink-env
  auth: "admin:pw"
  image: "icr.io/ns/img:1"
watsonx:
  project_id: "p-1"
opensearch:
  hosts: "https://os:9200"
"""


@pytest.fixture
def cfg(tmp_path, monkeypatch):
    """A config loaded from a file, with the shell deliberately empty.

    The environment wins over the file — that is how the instructor's scripts
    feed the same code from the environment — so a stray WS_ID in the developer's own
    shell would otherwise decide what these tests assert.
    """
    for name in ("WS_ID", "WS_VARIANT", "WS_PREFIX", "KAFKA_BOOTSTRAP_SERVERS", "SINK_TYPE"):
        monkeypatch.delenv(name, raising=False)

    def _load(body: str = MINIMAL, **kw):
        path = tmp_path / "lab.yaml"
        path.write_text(textwrap.dedent(body))
        return labconfig.load(path, **kw)


    return _load


# --- the id is the whole namespace -----------------------------------------
def test_a_one_digit_id_is_padded(cfg):
    """`7` and `07` must be the same student, or their names do not match."""
    assert cfg().student_id == "07"


def test_every_name_derives_from_the_id(cfg):
    c = cfg()
    assert c.app_name == "ws-07-simple"
    assert c.variant_topics() == ["ws.07.chunks", "ws.07.enriched"]
    assert c.opensearch_username == "student07"
    assert c.opensearch_index == "student07-chunks"
    assert c.environ()["KAFKA_CONSUMER_GROUP"] == "ws-07-simple"


def test_the_full_variant_adds_three_topics_and_keeps_the_first_two(cfg):
    """A student switching pipelines keeps reading the same chunks."""
    c = cfg(variant="full")
    assert c.variant_topics() == [
        "ws.07.chunks", "ws.07.enriched", "ws.07.policy", "ws.07.pii", "ws.07.rejected",
    ]
    assert c.app_name == "ws-07-full"
    assert c.environ()["JOB_MODULE"] == "pipeline.full_job"


def test_the_full_variant_turns_chaining_off_and_asks_for_more_memory(cfg):
    """Not tuning: its broadcast operator makes PyFlink's chaining optimizer
    throw at submit time, and every unchained Python operator costs a worker."""
    simple, full = cfg().environ(), cfg(variant="full").environ()
    assert simple["WS_CHAINING"] == "true" and full["WS_CHAINING"] == "false"
    assert simple["WS_TM_MEMORY"] == "1024m" and full["WS_TM_MEMORY"] == "2048m"


def test_the_instructor_can_ask_for_any_student(cfg):
    """`workshop.sh` fans out over the class through this same code."""
    assert cfg(student="3").app_name == "ws-03-simple"


# --- the bucket -------------------------------------------------------------
def test_the_pasted_crn_decides_the_bucket_name(cfg):
    """Names are globally unique, so a student whose suggested name was taken
    has a bucket the template does not describe."""
    c = cfg(MINIMAL.replace('bucket_crn: ""', 'bucket_crn: "crn:v1:a:b::bucket:my-own-bucket-md"'))
    assert c.bucket_name == "my-own-bucket-md"


def test_without_a_crn_the_template_is_only_a_suggestion(cfg):
    c = cfg(MINIMAL + '\ncos:\n  bucket_template: txc26-ws-{id}-uploads\n')
    assert c.bucket_name == "txc26-ws-07-uploads"
    # ...and it is exported as an explicit empty, which the dashboard reads as
    # "unknown" rather than falling back to somebody else's.
    assert c.environ()["COS_BUCKET_CRN"] == ""


# --- validation speaks YAML -------------------------------------------------
def test_problems_name_the_yaml_key_not_the_variable(cfg):
    c = cfg(MINIMAL.replace('id: "7"', 'id: ""'))
    assert any("student.id" in p for p in c.validate())
    assert not any("WS_ID" in p for p in c.validate())


def test_a_dimension_that_disagrees_with_the_model_is_caught_here(cfg, monkeypatch):
    """Otherwise it surfaces on the first embedding response, by which time the
    job has consumed the whole topic."""
    monkeypatch.setenv("EMBEDDING_DIMENSION", "384")
    problems = cfg().validate()
    assert any("384" in p and "768" in p for p in problems)


def test_a_missing_ca_file_is_a_problem_not_a_timeout_later(cfg):
    c = cfg(MINIMAL.replace("ca_file: ./certs/root-ca.pem", "ca_file: ./certs/nope.pem"))
    assert any("kafka.ca_file" in p for p in c.validate())


# --- rendering --------------------------------------------------------------
def test_render_substitutes_from_the_environment(cfg):
    assert labconfig.render("app=${APP_NAME}", cfg().environ()) == "app=ws-07-simple"


def test_render_refuses_an_unset_name():
    """The one thing ``envsubst`` got wrong: it substitutes an empty string, so
    a typo produced a FlinkApplication that CMF accepted, that started, and that
    then read from a topic called "" forever."""
    with pytest.raises(labconfig.LabConfigError) as exc:
        labconfig.render("topic=${KAFKA_TYPOED_TOPIC}", {"APP_NAME": "x"})
    assert "KAFKA_TYPOED_TOPIC" in str(exc.value)


def test_the_workshop_descriptor_renders_completely(cfg):
    """Every placeholder in the real template must be a name this produces —
    which is the check that stops the two drifting apart."""
    template = (labconfig.PROJECT_ROOT / "flink" / "application-workshop.json.tmpl").read_text()
    rendered = labconfig.render(template, cfg(variant="full").environ())
    assert "${" not in rendered
    import json
    assert json.loads(rendered)["metadata"]["name"] == "ws-07-full"


# --- the instructor's fan-out and the non-workshop deploy -------------------
def test_a_previous_student_in_the_environment_does_not_win(cfg, monkeypatch):
    """The bug this rule exists for.

    ``ws_student_env`` exports one student's names before resolving the next,
    so if the environment won over the derived names, student 07 would be
    handed student 03's topics — and nothing would say so.
    """
    monkeypatch.setenv("KAFKA_CHUNKS_TOPIC", "ws.03.chunks")
    monkeypatch.setenv("APP_NAME", "ws-03-simple")
    env = cfg(student="07").environ()
    assert env["KAFKA_CHUNKS_TOPIC"] == "ws.07.chunks"
    assert env["APP_NAME"] == "ws-07-simple"


def test_per_variant_sizing_cannot_be_inherited_either(cfg, monkeypatch):
    """Same shape of bug, and it ends in a TaskManager that will not start:
    a process that resolved `simple` first exports WS_TM_MEMORY=1024m, and a
    `full` deploy resolved afterwards would take it back in as an override."""
    monkeypatch.setenv("WS_TM_MEMORY", "1024m")
    monkeypatch.setenv("WS_CHAINING", "true")
    env = cfg(variant="full").environ()
    assert env["WS_TM_MEMORY"] == "2048m"
    assert env["WS_CHAINING"] == "false"


def test_standalone_takes_the_names_from_the_environment(cfg, monkeypatch):
    """The non-workshop deploy owns one pipeline under fixed names rather than
    thirty under derived ones, so there the names *are* configuration."""
    monkeypatch.setenv("APP_NAME", "docling-chunk-full")
    monkeypatch.setenv("JOB_MODULE", "pipeline.full_job")
    monkeypatch.setenv("KAFKA_CHUNKS_TOPIC", "docling.chunks")
    env = cfg(standalone=True, variant="full").environ()
    assert env["APP_NAME"] == "docling-chunk-full"
    assert env["KAFKA_CHUNKS_TOPIC"] == "docling.chunks"
    # ...but the sizing is still the variant's, not the environment's.
    assert env["WS_TM_MEMORY"] == "2048m"


def test_the_standalone_descriptor_renders_completely(cfg, monkeypatch):
    for name, value in {
        "APP_NAME": "docling-chunk-enrich", "JOB_MODULE": "pipeline.enrich_job",
        "KAFKA_CHUNKS_TOPIC": "docling.chunks", "KAFKA_OUTPUT_TOPIC": "docling.chunks.enriched",
    }.items():
        monkeypatch.setenv(name, value)
    template = (labconfig.PROJECT_ROOT / "flink" / "application.json.tmpl").read_text()
    rendered = labconfig.render(template, cfg(standalone=True).environ())
    assert "${" not in rendered


# --- the workbench, derived from the service URL ----------------------------
@pytest.mark.parametrize(
    "service, workbench",
    [
        # The shape a student pastes: the same instance id under both names.
        (
            "https://api.aws-c1.dcls.saas.ibm.com/20260612-2107-5478-5050-afa2bbefdd96",
            "https://workbench.aws-c1.dcls.saas.ibm.com/instances/20260612-2107-5478-5050-afa2bbefdd96",
        ),
        # The test SaaS environment is the same shape, one label deeper.
        (
            "https://api.c1.dcls.test.saas.ibm.com/20260609-1214-4430-4068-54bbb3423e22",
            "https://workbench.c1.dcls.test.saas.ibm.com/instances/20260609-1214-4430-4068-54bbb3423e22",
        ),
        # A trailing slash is what a browser leaves behind on a copied URL.
        (
            "https://api.aws-c1.dcls.saas.ibm.com/inst-1/",
            "https://workbench.aws-c1.dcls.saas.ibm.com/instances/inst-1",
        ),
    ],
)
def test_the_workbench_follows_from_the_service_url(service, workbench):
    assert labconfig.docling_workbench_url(service) == workbench


@pytest.mark.parametrize(
    "service",
    [
        "",
        "http://localhost:5001",                      # docling-serve on the laptop
        "https://api.aws-c1.dcls.saas.ibm.com",       # a service URL with no instance
        "https://workbench.aws-c1.dcls.saas.ibm.com/instances/inst-1",  # already the UI
        "not a url",
    ],
)
def test_no_workbench_is_empty_rather_than_invented(service):
    """The callers fall back to the service URL, which at least identifies the
    deployment — a composed link to a host that does not exist would not."""
    assert labconfig.docling_workbench_url(service) == ""
