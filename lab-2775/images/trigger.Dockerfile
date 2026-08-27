# The COS-event trigger, for IBM Code Engine.
#
# Build from the project root, which is the context:
#   podman build --platform linux/amd64 -f images/trigger.Dockerfile -t <ref> .
#
# Nothing about this image resembles the pipeline image beside it: no Flink, no
# JVM, no cross-architecture toolchain. It is a Python web app — a slim Python
# base and three pure-Python wheels: ~158 MB, of which 123 MB is the base, and
# seconds to build on any architecture.
#
# It copies exactly one package — src/docling_trigger — and installs exactly one
# dependency group, so the four other components in the context cannot leak into
# it. tests/test_images.py enforces both halves of that.

# ---- builder: resolve the environment, and keep uv out of the result --------
# Two stages for one reason: uv is ~60 MB and is a build tool. A single stage
# would ship it, which is most of the difference between a 150 MB image and a
# 250 MB one for an app whose own dependencies are three pure-Python wheels.
FROM python:3.12-slim AS builder

# Pinned, and not below the uv that wrote uv.lock: an older uv refuses a
# lockfile revision it does not know. Same version as the pipeline image.
COPY --from=ghcr.io/astral-sh/uv:0.11.4 /uv /usr/local/bin/

ENV UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Dependencies only, from the same lockfile the rest of the lab is pinned by —
# never the project itself, hence `--no-install-project`: the package is copied
# in the final stage and reached through PYTHONPATH, so hatchling is never asked
# to build a wheel in a layer with no source in it. `--no-default-groups --group
# trigger` is what keeps this to fastapi, uvicorn and httpx: the pipeline's
# apache-flink is in a different group and is not resolved here at all.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-default-groups --group trigger

# ---- final image ------------------------------------------------------------
# The same base, so the venv's interpreter symlinks still resolve after the copy.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH=/app/.venv/bin:$PATH \
    PYTHONPATH=/app/src \
    PORT=8080

WORKDIR /app

# Code Engine runs containers as a non-root, arbitrary UID, so everything has to
# be readable by group 0. The ownership is set by the COPYs themselves rather
# than by a `chown -R` afterwards: a recursive chown rewrites every file it
# touches, which writes a second copy of the whole venv into its own layer —
# 35 MB, for an image whose own dependencies are three pure-Python wheels.
RUN useradd --create-home --uid 1001 --gid 0 trigger

COPY --from=builder --chown=1001:0 /app/.venv /app/.venv
COPY --chown=1001:0 src/docling_trigger ./src/docling_trigger

USER 1001

EXPOSE 8080
# PORT is injected by Code Engine; the module reads it (src/docling_trigger/app.py).
CMD ["python", "-m", "docling_trigger.app"]
