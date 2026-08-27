"""The two images must stay narrow.

Both Dockerfiles now build from the project root, so the context holds all five
packages under ``src/``. What keeps the pipeline image free of the trigger — and
the trigger image free of pyflink — is no longer the shape of the build context
but two explicit choices per file: one narrow ``COPY``, and one dependency
group. Nothing warns you when either widens; a 1.6 GB trigger image looks like a
slow build, not like a bug. So they are asserted here.

The runtime half of the same guarantee (``import pipeline`` must fail inside the
trigger image) is a CI step, because it needs a built image.
"""

from __future__ import annotations

import re
from pathlib import Path

IMAGES = Path(__file__).resolve().parents[1] / "images"

# The one package each image is allowed to carry, and the group it installs.
EXPECTED = {
    "pipeline.Dockerfile": ("src/pipeline", "image"),
    "trigger.Dockerfile": ("src/docling_trigger", "trigger"),
}


def _instructions(text: str) -> str:
    """The Dockerfile with its comments stripped.

    Prose about ``--group`` in a comment is not an install, and the checks below
    would otherwise fail on a file that explains itself.
    """
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def _copied_paths(text: str) -> list[str]:
    """Every source path a COPY brings in from the build context.

    ``COPY --from=...`` is excluded: those copy out of an earlier stage or a
    pinned image (uv), not out of the context, so they cannot widen anything.
    """
    paths: list[str] = []
    for line in text.splitlines():
        line = line.strip()
        if not line.upper().startswith("COPY "):
            continue
        parts = line.split()[1:]
        if any(p.startswith("--from=") for p in parts):
            continue
        paths.extend(p for p in parts[:-1] if not p.startswith("--"))
    return paths


def test_each_image_copies_only_its_own_package():
    for name, (package, _group) in EXPECTED.items():
        copied = _copied_paths(_instructions((IMAGES / name).read_text()))
        src_paths = [p for p in copied if p.startswith("src")]
        assert src_paths == [package], f"{name} copies {src_paths}, expected only {package!r}"
        # A bare `COPY . ` or `COPY src/` would satisfy the check above by
        # accident, so refuse the wildcards outright.
        assert "." not in copied and "./" not in copied, f"{name} copies the whole context"
        assert "src" not in copied and "src/" not in copied, f"{name} copies all of src/"


def test_each_image_installs_only_its_own_group():
    for name, (_package, group) in EXPECTED.items():
        text = (IMAGES / name).read_text()
        groups = re.findall(r"--group[= ]([\w-]+)", _instructions(text))
        assert groups == [group], f"{name} installs groups {groups}, expected only {group!r}"
        assert "--no-default-groups" in text, f"{name} must pass --no-default-groups"
        # Without this, hatchling is asked to build the project in a layer that
        # has no source in it yet — and the build fails at `uv sync`.
        assert "--no-install-project" in text, f"{name} must pass --no-install-project"
