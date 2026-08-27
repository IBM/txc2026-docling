# PyFlink image for Confluent Manager for Apache Flink (CMF) and for the local
# podman/docker stack.
#
# Build from the project root, which is the context:
#   podman build --platform linux/amd64 -f images/pipeline.Dockerfile -t <ref> .
#
# It copies exactly one package — src/pipeline — so the context holding the
# other four components cannot leak into it. tests/test_images.py enforces that.
#
# The cp-flink runtime base is RHEL 9 with NO system Python, no package manager,
# and a JRE (not a JDK). So we build in a matching RHEL 9 userspace that does
# have a toolchain, and copy the result across:
#   * `uv` installs a self-contained, relocatable CPython + virtualenv under a
#     fixed path, so the venv's interpreter symlinks stay valid after the copy.
#     This mirrors Confluent's PyFlink packaging guidance.
#   * gcc + JDK headers are needed because PyFlink's `pemja` dependency only
#     publishes a manylinux **x86_64** wheel — on linux/arm64 (Apple Silicon)
#     it is compiled from source against JNI headers. It is the only thing left
#     that needs a toolchain at all (see the `image` group in pyproject.toml).
FROM redhat/ubi9:9.6 AS builder

RUN dnf install -y --setopt=install_weak_deps=False \
        gcc gcc-c++ make java-11-openjdk-devel \
 && dnf clean all
ENV JAVA_HOME=/usr/lib/jvm/java-11

# Pinned, and not below the uv that wrote uv.lock: an older uv refuses a
# lockfile revision it does not know.
COPY --from=ghcr.io/astral-sh/uv:0.11.4 /uv /uvx /usr/local/bin/

ENV UV_PYTHON_INSTALL_DIR=/opt/flink/pyflink/python \
    UV_LINK_MODE=copy \
    UV_PYTHON=3.11 \
    VIRTUAL_ENV=/opt/flink/pyflink/.venv \
    UV_PROJECT_ENVIRONMENT=/opt/flink/pyflink/.venv
WORKDIR /opt/flink/pyflink

# Standalone, relocatable CPython (not the distro python: there isn't one).
RUN uv python install 3.11

# Dependencies only, from the lockfile — never the project itself, hence
# `--no-install-project`: `pipeline` is copied to src/ below and reached through
# PYTHONPATH, so a code change rebuilds one layer instead of reinstalling
# anything, and hatchling is never asked to build a wheel in a layer that has no
# source in it yet. `--no-default-groups`
# leaves the laptop's groups out; what `image` holds is three packages, no
# torch, no transformers, no docling, because nothing here converts or chunks.
# That is also why there is no model, no tokenizer cache and no CPU-wheel index
# dance, and why a --platform linux/amd64 build under qemu has no native code
# to trip over.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-default-groups --group image

# Strip what never runs:
#   pyflink/lib, pyflink/opt — the pip wheel bundles a whole Flink distribution,
#       but FLINK_HOME=/opt/flink is set by the cp-flink base and
#       `_find_flink_home()` checks it first, so these are a shadow copy of jars
#       the runtime already has. Verified: the JVM gateway still starts.
RUN SP="$VIRTUAL_ENV/lib/python3.11/site-packages" \
 && rm -rf "$SP/pyflink/lib" "$SP/pyflink/opt" \
 && find "$VIRTUAL_ENV" -name '__pycache__' -type d -prune -exec rm -rf {} + \
 && find "$VIRTUAL_ENV" -name '*.pyc' -delete

# Only the pipeline package. The other four under src/ are laptop-side or
# belong to another image: producer converts and chunks, labtools and inspector
# are the laptop's, docling_trigger is a different deployment entirely.
COPY src/pipeline ./src/pipeline

# ---- final image ----
FROM confluentinc/cp-flink:2.1.3-cp2

USER root
COPY --from=builder --chown=flink:flink /opt/flink/pyflink/ /opt/flink/pyflink/

# Flink Kafka connector jar (run ./scripts/fetch_jars.sh first).
COPY --chown=flink:flink jars/ /opt/flink/lib/

ENV PYTHONPATH=/opt/flink/pyflink/src

USER flink
