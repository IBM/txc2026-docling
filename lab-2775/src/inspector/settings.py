"""Where the dashboard looks, and how it authenticates.

Two profiles, matching the two ways the pipeline is run in this repo:

* ``local``  — the compose stack (``make up``): PLAINTEXT broker on
  ``localhost:29092``, the Flink session cluster's REST API on
  ``localhost:8081``, OpenSearch on ``localhost:9200`` with security off.
* ``hosted`` — the workshop's cluster, as described by the student's ``lab.yaml``:
  SASL_SSL brokers, and Flink under CMF (no reachable Flink REST, so the
  dashboard falls back to CMF's application status — see ``flink_probe``), with
  documents arriving through the COS bucket named by ``COS_BUCKET_CRN`` (see
  ``cos.py``, which composes its console link). In the workshop that CRN is
  exported per student by ``./pipeline.sh inspect`` rather than read out of
  ``lab.yaml`` — including as an explicit empty, see ``_EXPLICIT_EMPTY_KEYS``.

The chosen profile is pushed into ``os.environ`` before anything else runs.
That is deliberate: the repo's own helpers (``labtools.kafka``,
``pipeline.config``) read their configuration from the environment, and reusing
them unchanged is what keeps this dashboard from drifting away from the
pipeline it inspects. ``labtools.config`` is what turns the student's
``lab.yaml`` into that environment in the first place.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from labtools.config import docling_workbench_url

from . import cos

# lab-2775/ — the project root, two levels up from src/inspector/.
REPO_ROOT = Path(__file__).resolve().parents[2]


def _lab_yaml() -> dict[str, str]:
    """The student's ``lab.yaml``, resolved the same way every script resolves it.

    Through :mod:`labtools.config` rather than by reading the file, so the
    dashboard shows the names a deploy would actually use — the derived topics,
    the derived index — instead of a second opinion about them.

    It is allowed to fail. ``./pipeline.sh inspect`` has already exported the
    whole environment before starting Streamlit, so a dashboard launched that
    way needs nothing from here — and the workshop's own tooling, which is
    environment-based, has no lab.yaml at all.
    """
    try:
        from labtools import config as labconfig

        return labconfig.load().environ()
    except Exception:  # noqa: BLE001 — an unreadable config must not be fatal
        return {}


def _dotenv(path: Path) -> dict[str, str]:
    """Minimal ``.env`` reader — a shell-sourced KEY=VALUE file.

    Kept for a checkout that still has one beside it; the lab itself has none.
    """
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            values[key] = value
    return values


# The local stack's addresses as seen *from the laptop*. Not .env.local: that
# file holds in-compose addresses (kafka:9092) which do not resolve here.
LOCAL_DEFAULTS = {
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:29092",
    "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
    "KAFKA_API_KEY": "",
    "KAFKA_API_SECRET": "",
    "KAFKA_CA_LOCATION": "",
    "OPENSEARCH_HOSTS": "http://localhost:9200",
    "OPENSEARCH_USERNAME": "",
    "OPENSEARCH_PASSWORD": "",
    "OPENSEARCH_VERIFY_CERTS": "false",
    # Security is off in the compose stack, so there is no private CA to trust
    # and the hosted lab's path must not follow the profile switch across.
    "OPENSEARCH_CA_LOCATION": "",
    "FLINK_REST_URL": "http://localhost:8081",
    # No bucket and no events locally: `make ingest` posts to a docling-serve
    # running on the laptop. An empty COS_BUCKET_URL is what the topology reads
    # as "this profile is not the event-driven one" — so the CRN has to go too,
    # or it would be composed back into one.
    "COS_BUCKET_CRN": "",
    "COS_BUCKET_REGION": "",
    "COS_BUCKET_URL": "",
    "COS_BUCKET": "",
    "COS_INSTANCE_CRN": "",
    "DOCLING_SERVICE_URL": "http://localhost:5001",
    "WS_LAB_ENV": "",
}

# Keys a profile is allowed to set. Anything else in .env (image names, Docling
# credentials, ...) is none of the dashboard's business.
_ENV_KEYS = (
    "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_SECURITY_PROTOCOL", "KAFKA_SASL_MECHANISM",
    "KAFKA_API_KEY", "KAFKA_API_SECRET", "KAFKA_CA_LOCATION",
    "KAFKA_CHUNKS_TOPIC", "KAFKA_OUTPUT_TOPIC", "KAFKA_CONSUMER_GROUP",
    "KAFKA_POLICY_TOPIC", "KAFKA_QUARANTINE_TOPIC", "KAFKA_REJECTED_TOPIC",
    "PIPELINE_MIN_CHARS", "PIPELINE_DROP_LOW_QUALITY", "PIPELINE_DEDUP",
    "PIPELINE_DEDUP_TTL_HOURS", "PIPELINE_EMBED", "PIPELINE_PARALLELISM",
    "SINK_TYPE",
    "WATSONX_URL", "EMBEDDING_MODEL_ID", "EMBEDDING_DIMENSION",
    "OPENSEARCH_HOSTS", "OPENSEARCH_INDEX", "OPENSEARCH_USERNAME",
    "OPENSEARCH_PASSWORD", "OPENSEARCH_VERIFY_CERTS", "OPENSEARCH_CA_LOCATION",
    # The Ask tab answers out of the index, which means it embeds the question
    # with the same model the pipeline embedded the chunks with and asks a chat
    # model to answer from what came back. That needs the credentials the *job*
    # has — this is the one panel here that calls a service rather than reading
    # one, and it is still read-only with respect to the pipeline.
    "WATSONX_APIKEY", "WATSONX_PROJECT_ID", "WATSONX_SPACE_ID",
    "WATSONX_LLM_MODEL_ID", "WATSONX_LLM_MAX_TOKENS",
    "EMBEDDING_MAX_TOKENS",
    "CMF_URL", "CMF_ENVIRONMENT", "CMF_AUTH",
    # Which application to ask CMF about. Per student, not per job — the
    # workshop deploys one FlinkApplication each (`ws-NN`), so this moves with
    # the topic names or the Flink panel contradicts every panel beside it.
    "CMF_APPLICATION",
    "FLINK_REST_URL",
    # The event-driven front of the pipeline: a document is uploaded to a COS
    # bucket, and the object-created event triggers a Docling job. The dashboard
    # cannot probe any of it — these are the links it shows so a student can get
    # to the console and the service.
    "COS_BUCKET_CRN", "COS_BUCKET_REGION", "COS_ENDPOINT",
    "COS_BUCKET_URL", "COS_BUCKET",
    # The COS *instance* — everything a bucket CRN is except the bucket name.
    # Enough to address the console, because the instance is the URL path and
    # the bucket is only a query parameter.
    "COS_INSTANCE_CRN",
    "DOCLING_SERVICE_URL",
    # Which run of the lab this is (``scripts/lab_env.sh``). The dashboard shows
    # it rather than acting on it: it is what tells a student looking at the
    # Configuration tab whether the bucket names around them are the dev set or
    # the real one.
    "WS_LAB_ENV",
)

# Keys where an exported *empty* value is a statement rather than an absence.
#
# ``./pipeline.sh inspect`` — and the instructor's fan-out, which drives it once
# per student — export the whole per-student environment, and for the bucket that can legitimately
# come out empty — student NN's CRN is not known. Applying the usual "empty
# means unset" rule there would fall back to the config file, which holds
# whoever's bucket that machine was set up with, and every student's dashboard
# would offer an upload button pointing at the instructor's bucket. Showing no
# link is better than silently linking to the wrong bucket.
_EXPLICIT_EMPTY_KEYS = ("COS_BUCKET_CRN", "COS_BUCKET", "COS_BUCKET_URL")

# The local stack is a *development* target: it is a compose file that is not
# published with the lab, and a student has no way to start it. So it is not
# offered unless LAB_DEV=1 says this is a development checkout — otherwise the
# dashboard would show a "Target system" choice with one working answer, and
# a student who picked the other would spend ten minutes debugging localhost.
DEV_MODE = os.environ.get("LAB_DEV", "").strip().lower() in ("1", "true", "yes")
PROFILES = ("local", "hosted") if DEV_MODE else ("hosted",)

# The shell's own variables, snapshotted at import — before any profile has been
# applied. ``apply()`` writes into ``os.environ``, so reading it back later would
# make the profile you looked at first win over the one you just selected: switch
# local -> hosted and the bootstrap servers would stay ``localhost:29092``.
_SHELL_ENV = {
    k: v
    for k, v in os.environ.items()
    if k in _ENV_KEYS and (v or k in _EXPLICIT_EMPTY_KEYS)
}


@dataclass(frozen=True)
class Settings:
    profile: str
    values: dict[str, str] = field(default_factory=dict)

    def get(self, key: str, default: str = "") -> str:
        return self.values.get(key) or default

    # --- the three back ends the dashboard talks to -----------------------
    @property
    def bootstrap(self) -> str:
        return self.get("KAFKA_BOOTSTRAP_SERVERS")

    @property
    def flink_rest_url(self) -> str:
        return self.get("FLINK_REST_URL")

    @property
    def cmf(self) -> tuple[str, str, str] | None:
        url, env, auth = self.get("CMF_URL"), self.get("CMF_ENVIRONMENT"), self.get("CMF_AUTH")
        return (url, env, auth) if url and env else None

    @property
    def opensearch_hosts(self) -> str:
        return self.get("OPENSEARCH_HOSTS")

    @property
    def opensearch_verify(self) -> bool:
        return self.get("OPENSEARCH_VERIFY_CERTS", "true").lower() == "true"

    @property
    def opensearch_ca(self) -> str:
        """The CA that signed the cluster's certificate, as an absolute path.

        The workshop cluster uses a private CA exactly as the Kafka brokers do.
        Without it the client refuses the certificate and the OpenSearch and
        Ask tabs both read "not reachable", which is true but unhelpful.
        """
        return self.get("OPENSEARCH_CA_LOCATION")

    @property
    def watsonx_ready(self) -> bool:
        """Whether the Ask tab can call watsonx.ai at all."""
        return bool(self.get("WATSONX_APIKEY")) and bool(
            self.get("WATSONX_PROJECT_ID") or self.get("WATSONX_SPACE_ID")
        )

    # --- the two things upstream of Kafka, which are links, not probes -----
    @property
    def cos_bucket_url(self) -> str:
        """The bucket's web console — where a student uploads a document."""
        return self.get("COS_BUCKET_URL")

    @property
    def cos_bucket(self) -> str:
        return self.get("COS_BUCKET")

    @property
    def docling_url(self) -> str:
        """The API endpoint — what ``scripts/saas_ingest.py`` submits to."""
        return self.get("DOCLING_SERVICE_URL")

    @property
    def docling_ui_url(self) -> str:
        """The instance's workbench — the same instance under a different
        hostname, so it follows from the API endpoint rather than being
        configured beside it (:func:`labtools.config.docling_workbench_url`).
        A service with no workbench — docling-serve on the laptop — falls back
        to the API URL, which at least identifies the deployment."""
        return docling_workbench_url(self.docling_url) or self.docling_url

    @property
    def lab_env(self) -> str:
        """``dev`` or ``prod`` — which run of the lab the names around this
        student belong to. Display only; nothing here branches on it."""
        return self.get("WS_LAB_ENV")

    @property
    def cos_crn(self) -> str:
        return self.get("COS_BUCKET_CRN")

    @property
    def cos_error(self) -> str:
        """Why a configured CRN produced no link, if it did not."""
        return self.get("COS_BUCKET_CRN_ERROR")

    @property
    def event_driven(self) -> bool:
        """True when documents arrive through the bucket rather than a script."""
        return bool(self.cos_bucket_url)

    def apply(self) -> None:
        """Publish this profile into ``os.environ``.

        Everything downstream — ``labtools.kafka.client_config``, the
        ``pipeline.config`` dataclasses the topology is derived from — reads
        the environment, exactly as the pipeline's own scripts do.
        """
        for key in _ENV_KEYS:
            value = self.values.get(key)
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def load(profile: str) -> Settings:
    """Build the settings for ``profile`` from lab.yaml, then the shell."""
    if profile not in PROFILES:
        raise ValueError(f"profile must be one of {PROFILES} (got {profile!r})")

    resolved = {**_dotenv(REPO_ROOT / ".env"), **_lab_yaml()}
    values = {k: v for k, v in resolved.items() if k in _ENV_KEYS}

    # A real environment variable wins over the file, so
    # `KAFKA_CHUNKS_TOPIC=other streamlit run app.py` works the way it does for
    # every other script in the repo.
    values.update(_SHELL_ENV)

    if profile == "local":
        # Topic names and stage toggles still come from .env — it is the same
        # job — but every address and credential is the local one, and that
        # holds even when the shell has `. ./.env` sourced into it. "Local"
        # meaning "localhost" is worth more than one more override rule.
        values.update(LOCAL_DEFAULTS)
        for key in ("CMF_URL", "CMF_AUTH", "CMF_ENVIRONMENT"):
            values.pop(key, None)
    else:
        values.setdefault("FLINK_REST_URL", "")

    # The bucket link, from the CRN a student copies out of the console. Done
    # here rather than at each use so that `apply()` publishes the composed URL
    # and everything downstream — the topology, the Configuration tab — sees one
    # resolved value. An explicit COS_BUCKET_URL wins: a lab that hands out the
    # link should not have to be reverse-engineered into a CRN.
    crn = values.get("COS_BUCKET_CRN", "")
    if not crn and values.get("COS_BUCKET"):
        # We know *which* bucket this student uploads to but have no CRN for it:
        # nobody pasted one and there is no COS_INSTANCE_CRN to derive it from.
        # The console link is still buildable, because the instance is the URL
        # path and the bucket is only a query parameter — so any CRN from the
        # same instance serves, and the config file's own is one. This is read
        # from the file rather than from `values`, which the per-student
        # environment may have deliberately cleared; what must not follow it
        # back is the *name*, and it cannot: this branch only runs when
        # COS_BUCKET is already set, so the block below leaves it alone.
        crn = values.get("COS_INSTANCE_CRN") or resolved.get("COS_BUCKET_CRN", "")
    if crn:
        try:
            _, name = cos.parse_crn(crn)
            # `setdefault` is not enough: `.env` carries these keys as empty
            # strings, and an empty override is not an override.
            if not values.get("COS_BUCKET"):
                values["COS_BUCKET"] = name
            if not values.get("COS_BUCKET_URL"):
                values["COS_BUCKET_URL"] = cos.bucket_url(
                    crn,
                    region=values.get("COS_BUCKET_REGION", ""),
                    endpoint=values.get("COS_ENDPOINT", ""),
                    bucket=values.get("COS_BUCKET", ""),
                )
        except ValueError as exc:
            # A malformed CRN is a typo in `.env`, not a reason to refuse to
            # start: the dashboard says so and falls back to the script path.
            values["COS_BUCKET_CRN_ERROR"] = str(exc)

    # A relative CA path in .env is relative to the repo root, and the dashboard
    # is not started from there. Both CAs, for the same reason — and note that
    # OPENSEARCH_CA_LOCATION in a *deployed job's* environment is the container
    # path the podTemplate mounts, which is why the two are never shared.
    for key in ("KAFKA_CA_LOCATION", "OPENSEARCH_CA_LOCATION"):
        ca = values.get(key)
        if ca and not os.path.isabs(ca):
            values[key] = str((REPO_ROOT / ca).resolve())

    return Settings(profile=profile, values=values)
