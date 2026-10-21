"""The instructors' file for this VM, and what the bootstrap does with it.

Before the class, the instructors put one file on every lab VM, outside the
checkout so that no re-run and no ``git pull`` touches it::

    ~/lab-2775.yaml

It has the shape of ``lab.yaml`` itself — the same sections, the same keys —
and holds everything about this student that is not theirs to choose: their
id, their OpenSearch password, and the brokers, Flink control plane, watsonx.ai
project and accounts this VM was given. The workshop runs more than one of
each to spread the load, and which one a student is on is decided by the file,
not by anything the student types. ``workshop.yaml.example`` is the format.

One thing in it has no place in ``lab.yaml``: ``kafka.ca_pem``, the broker CA
inline. The two clusters have different CAs, and one file per VM is simpler to
hand out than a file and a certificate that must not be mixed up.

``install`` is what the bootstrap runs when the file is there:

1. copies it to ``workshop.yaml`` beside ``lab.yaml`` — every time, because
   it belongs to the instructors: re-sending it and re-running the bootstrap
   is how a rotated credential reaches a VM mid-class;
2. writes ``kafka.ca_pem`` to ``certs/kafka-ca.crt``;
3. makes sure ``lab.yaml`` starts with ``include: workshop.yaml``, which is
   how :mod:`labtools.config` comes to read it. ``lab.yaml`` itself is
   otherwise left alone, and what the student writes there still wins.

The student's IBM Cloud sign-in is not in this file. It is for a browser, not
for a script, so the instructors hand it out as a page of its own beside this
one (``~/ibmcloud-signin.html``), which nothing here reads.

    uv run --frozen python -m labtools.handout [~/lab-2775.yaml]
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from labtools.config import PROJECT_ROOT, LabConfigError, _dig, _read_yaml, resolve_include

HANDOUT = Path.home() / "lab-2775.yaml"
WORKSHOP_NAME = "workshop.yaml"
INCLUDE_LINE = f"include: {WORKSHOP_NAME}"
KAFKA_CA = Path("certs") / "kafka-ca.crt"


def ensure_include(lab_yaml: Path) -> str:
    """Prepend the include line unless ``lab.yaml`` already has one.

    Textual, not a YAML round-trip: the file is the student's, and a dump would
    take every comment in it with it.
    """
    if not lab_yaml.is_file():
        return "missing"
    current = _read_yaml(lab_yaml).get("include")
    if current is not None:
        target = resolve_include(lab_yaml, current)
        return "present" if target == lab_yaml.parent / WORKSHOP_NAME else f"other:{current}"
    text = lab_yaml.read_text()
    lab_yaml.write_text(
        "# The workshop's values for this VM — written by the bootstrap, not by you.\n"
        f"{INCLUDE_LINE}\n\n{text}"
    )
    return "added"


def install(src: Path, root: Path = PROJECT_ROOT) -> dict[str, str]:
    """Put ``src`` in place under ``root``. Returns what it found and did."""
    text = src.read_text()
    data = _read_yaml(src)
    sid = str(_dig(data, "student.id") or "").strip()
    if not sid:
        raise LabConfigError(f"{src}: no student.id — this is not a LAB-2775 handout")

    # Holds a password and the Kafka secret: readable by this user only.
    workshop = root / WORKSHOP_NAME
    workshop.write_text(text)
    os.chmod(workshop, 0o600)
    done = {"id": sid, "workshop": str(workshop)}

    pem = str(_dig(data, "kafka.ca_pem") or "").strip()
    if pem:
        if "BEGIN CERTIFICATE" not in pem:
            raise LabConfigError(f"{src}: kafka.ca_pem is not a PEM certificate")
        ca = root / KAFKA_CA
        ca.parent.mkdir(parents=True, exist_ok=True)
        ca.write_text(pem + "\n")
        done["ca"] = str(ca)

    lab_yaml = root / "lab.yaml"
    done["include"] = ensure_include(lab_yaml)
    if lab_yaml.is_file():
        own = str(_dig(_read_yaml(lab_yaml), "student.id") or "").strip()
        if own and own.lstrip("0") != sid.lstrip("0"):
            done["id_conflict"] = own
    return done


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="labtools.handout", description=__doc__.split("\n")[0])
    ap.add_argument("handout", nargs="?", default=str(HANDOUT))
    args = ap.parse_args(argv)
    src = Path(args.handout).expanduser()
    if not src.is_file():
        print(f"no handout at {src}", file=sys.stderr)
        return 1
    try:
        done = install(src)
    except LabConfigError as exc:
        print(f"handout: {exc}", file=sys.stderr)
        return 2

    print(f"  you are student {done['id']}")
    print(f"  workshop values in {WORKSHOP_NAME}" + (", broker CA in certs/" if "ca" in done else ""))
    if done["include"] == "added":
        print(f"  added '{INCLUDE_LINE}' to the top of lab.yaml")
    elif done["include"].startswith("other:"):
        print(f"  ! lab.yaml includes {done['include'][6:]}, not {WORKSHOP_NAME} — left as it is",
              file=sys.stderr)
    if "id_conflict" in done:
        print(f"  ! lab.yaml sets student.id to {done['id_conflict']}, which wins over the "
              f"handout's {done['id']}. Delete that line unless you mean it.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
