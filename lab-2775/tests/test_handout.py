"""``~/lab-2775.yaml`` — the instructors' file for one VM — and installing it.

What matters is that it is safe to repeat: the bootstrap installs it on every
run, so that re-sending the file reaches a VM mid-class, and none of that may
cost the student what they wrote in ``lab.yaml``.
"""

from __future__ import annotations

import pytest

from labtools import config as labconfig
from labtools import handout

PEM = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----"

HANDOUT = f"""\
student:
  id: "12"
  opensearch_password: "pw"
kafka:
  bootstrap_servers: "site-b:9094"
  api_key: "k"
  api_secret: "s"
  ca_pem: |
    {PEM.replace(chr(10), chr(10) + '    ')}
cmf:
  url: "https://cmf-b/cmf"
  auth: "admin:pw"
  image: "icr.io/ns/img:1"
watsonx:
  project_id: "p"
opensearch:
  hosts: "https://os:9200"
"""


@pytest.fixture
def lab(tmp_path, monkeypatch):
    for name in ("WS_ID", "WS_VARIANT", "KAFKA_BOOTSTRAP_SERVERS"):
        monkeypatch.delenv(name, raising=False)
    root = tmp_path / "lab"
    root.mkdir()
    src = tmp_path / "lab-2775.yaml"
    src.write_text(HANDOUT)
    return root, src


def test_install_writes_the_workshop_file_the_ca_and_the_include(lab):
    root, src = lab
    (root / "lab.yaml").write_text("# mine\ndocling:\n  api_key: dk\n")
    done = handout.install(src, root)
    assert done["id"] == "12" and done["include"] == "added"
    assert (root / "workshop.yaml").read_text() == HANDOUT
    assert (root / "workshop.yaml").stat().st_mode & 0o077 == 0
    assert (root / "certs/kafka-ca.crt").read_text().strip() == PEM
    text = (root / "lab.yaml").read_text()
    assert text.splitlines()[1] == "include: workshop.yaml"
    assert "# mine" in text  # the student's comments survive

    cfg = labconfig.load(root / "lab.yaml")
    assert cfg.student_id == "12"
    assert cfg.environ()["KAFKA_BOOTSTRAP_SERVERS"] == "site-b:9094"


def test_install_twice_adds_the_include_once(lab):
    root, src = lab
    (root / "lab.yaml").write_text("include: workshop.yaml\n")
    assert handout.install(src, root)["include"] == "present"
    assert handout.install(src, root)["include"] == "present"
    assert (root / "lab.yaml").read_text().count("include:") == 1


def test_a_reissued_handout_replaces_the_old_one(lab):
    root, src = lab
    handout.install(src, root)
    src.write_text(HANDOUT.replace("site-b", "site-a"))
    handout.install(src, root)
    assert "site-a" in (root / "workshop.yaml").read_text()


def test_a_student_id_in_lab_yaml_is_reported_not_overwritten(lab):
    root, src = lab
    (root / "lab.yaml").write_text('student:\n  id: "07"\n')
    done = handout.install(src, root)
    assert done["id_conflict"] == "07"
    assert 'id: "07"' in (root / "lab.yaml").read_text()


def test_a_file_without_an_id_is_refused(lab):
    root, src = lab
    src.write_text("kafka:\n  api_key: k\n")
    with pytest.raises(labconfig.LabConfigError):
        handout.install(src, root)
    assert not (root / "workshop.yaml").exists()


def test_the_example_is_a_valid_handout(tmp_path):
    root = tmp_path / "lab"
    root.mkdir()
    done = handout.install(labconfig.PROJECT_ROOT / "workshop.yaml.example", root)
    assert done["id"] == "07" and "ca" in done

