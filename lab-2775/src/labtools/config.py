"""``lab.yaml`` — the one file a student edits — and the environment it becomes.

Every component in this lab already reads its configuration from environment
variables: :mod:`pipeline.config` does, the ``FlinkApplication`` descriptor is
a template over them, a TaskManager is handed them by Kubernetes, and the
dashboard resolves them. That contract is not changed here. What changes is the
*human* end of it: a student edits YAML, and this module turns it into those
variables.

Three layers, later winning:

1. the defaults below — the resource envelope, the model ids, the names that
   are the same for every run of this lab;
2. ``lab.yaml``;
3. ``os.environ`` — which is how the instructor's fan-out feeds it: that
   tooling is environment-based and sources its own file with ``set -a``.

and then the per-student names, which are *derived* and never configured: a
topic, an application, a consumer group, an OpenSearch account and its index
all follow from ``student.id``. That is not tidiness. The three collisions that
matter are silent rather than loud — a shared consumer group makes Kafka split
the partitions between two students so most of the class sees no data at all;
a shared application name means one student's deploy replaces another's; a
shared output topic interleaves results nobody can then find. Deriving them
from one number is what makes those unrepresentable.

Used from the shell::

    eval "$(uv run --frozen python -m labtools.config env)"   # export lines
    uv run --frozen python -m labtools.config render flink/application-workshop.json.tmpl
    uv run --frozen python -m labtools.config check

and imported by the dashboard, which needs the same answers.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import sys
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

# lab-2775/ — three levels up from src/labtools/config.py.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = PROJECT_ROOT / "lab.yaml"

VARIANTS = ("simple", "full")


class LabConfigError(Exception):
    """A problem the student can fix in lab.yaml, phrased in its own terms."""


# --- what is the same for every run of this lab -----------------------------
# Not in lab.yaml, because nothing good comes of a student editing them, and a
# value that is never edited but always present is noise in a file that has to
# stay readable. Any of them can still be overridden from the environment,
# which is what the instructor's scripts use.
DEFAULTS: dict[str, str] = {
    # Naming.
    "WS_PREFIX": "ws",
    "WS_OS_USER_TEMPLATE": "student",
    # The two jobs.
    "WS_MODULE_SIMPLE": "pipeline.enrich_job",
    "WS_MODULE_FULL": "pipeline.full_job",
    # Both endings at once, and it is the reason SINK_TYPE takes a list: the
    # output topic is what makes the enrichment legible one record at a time,
    # the index is what the Ask tab retrieves from. A student who had to choose
    # would only ever see half of the lab.
    "SINK_TYPE": "kafka,opensearch",
    # Kafka as a pod inside the cluster sees it — not as the laptop does.
    "CMF_KAFKA_BOOTSTRAP": "kafka.confluent.svc.cluster.local:9071",
    "CMF_KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
    # EMBEDDING_MODEL_ID is a *watsonx.ai* id and it is regional: ca-tor serves
    # only granite-embedding-278m-multilingual (768), slate-125m-english-rtrvr-v2
    # (768), slate-30m-english-rtrvr-v2 (384) and multilingual-e5-large (1024).
    # Anything else is a 404 on the first batch, not a fallback.
    "EMBEDDING_MODEL_ID": "ibm/granite-embedding-278m-multilingual",
    "EMBEDDING_DIMENSION": "768",
    "EMBEDDING_MAX_TOKENS": "512",
    # A *HuggingFace* repo id, and not the same string as the one above: it only
    # ever supplies a tokenizer to the producers, so chunks are sized to the
    # same budget. Feeding one to the other fails.
    "CHUNK_TOKENIZER_ID": "ibm-granite/granite-embedding-278m-multilingual",
    # The region is not a formality: each watsonx datacentre publishes its own
    # catalogue, and a model the region does not serve is a 404 on the first
    # batch rather than a fallback.
    "WATSONX_URL": "https://ca-tor.ml.cloud.ibm.com",
    "WATSONX_MAX_BATCH": "32",
    "WATSONX_LLM_MODEL_ID": "meta-llama/llama-3-3-70b-instruct",
    "WATSONX_LLM_MAX_TOKENS": "700",
    # Secrets are Kubernetes Secrets, never literals in the descriptor.
    "WATSONX_SECRET_NAME": "watsonx-embeddings",
    "OPENSEARCH_SECRET_NAME": "opensearch-workshop",
    "OPENSEARCH_VERIFY_CERTS": "true",
    # Where the podTemplate mounts the OpenSearch CA — a path *inside the
    # container*, and so deliberately not OPENSEARCH_CA_LOCATION, which is read
    # by whatever process is running and on the laptop points at certs/.
    "WS_OS_CA_MOUNT": "/opt/flink/opensearch/root-ca.pem",
    # The per-student resource envelope. Requests are the real floor, because
    # requests are what the scheduler packs; the limit factors buy headroom on
    # top. CPU is over-committed on purpose: thirty students embedding 1000
    # chunks each are idle almost all of the time, so a request of 0.25 with a
    # limit factor of 8 still gives two cores to whoever is actually running.
    "WS_JM_MEMORY": "1024m",
    "WS_JM_CPU": "0.1",
    "WS_JM_CPU_LIMIT_FACTOR": "6",
    "WS_TM_CPU": "0.25",
    "WS_TM_CPU_LIMIT_FACTOR": "8",
    "WS_TM_MEM_LIMIT_FACTOR": "1.25",
    "WS_PULL_POLICY": "IfNotPresent",
    "WS_STATE": "running",
    "WS_EMBED": "true",
    # Stage knobs, both jobs.
    "PIPELINE_PARALLELISM": "2",
    "PIPELINE_MIN_CHARS": "40",
    "PIPELINE_DROP_LOW_QUALITY": "false",
    "PIPELINE_DEDUP": "true",
    "PIPELINE_DEDUP_TTL_HOURS": "24",
    "PIPELINE_EMBED": "true",
}

# Per-variant, and the reason is structural rather than tuning. The full
# pipeline's PII guard is a BroadcastProcessFunction; PyFlink's chaining
# optimizer cannot rewrite a broadcast-connected operator (it throws
# NoSuchFieldException: regularInput at submit time), so that job runs with
# chaining off — and every unchained Python operator gets its own Python worker
# process, each importing pyflink and beam before it does any work. Managed
# memory is what those processes are given, hence the larger fraction as well
# as the larger TaskManager.
#
# Managed memory is boxed in from both sides: Flink first carves metaspace and
# JVM overhead off the process size, then requires framework heap + framework
# off-heap + managed + network to fit in what is left *and* leave a non-zero
# task heap. At 1024m with the stock 256m metaspace a fraction of 0.55 asks for
# 636m out of 576m and the TaskManager refuses to start with
# IllegalConfigurationException before any code runs. The descriptor trims both
# deductions, which leaves ~704m at 1024m.
# The names that are *consequences* of a student id rather than settings. In
# the workshop they are derived and win over the environment — the fan-out
# exports one student's before resolving the next, so an environment that won
# would hand student 07 student 03's topics. In standalone mode there is no id,
# so these are exactly what the environment has to supply instead.
DERIVED_KEYS = (
    "STUDENT_ID", "WS_ID", "WS_VARIANT", "APP_NAME", "JOB_MODULE",
    "KAFKA_CHUNKS_TOPIC", "KAFKA_OUTPUT_TOPIC", "KAFKA_POLICY_TOPIC",
    "KAFKA_QUARANTINE_TOPIC", "KAFKA_REJECTED_TOPIC", "KAFKA_CONSUMER_GROUP",
    "OPENSEARCH_USERNAME", "OPENSEARCH_INDEX",
    "COS_BUCKET", "COS_BUCKET_CRN", "WS_SINK_TYPE",
)

PER_VARIANT: dict[str, dict[str, str]] = {
    "simple": {"WS_TM_MEMORY": "1024m", "WS_TM_MANAGED_FRACTION": "0.35", "WS_CHAINING": "true"},
    "full": {"WS_TM_MEMORY": "2048m", "WS_TM_MANAGED_FRACTION": "0.45", "WS_CHAINING": "false"},
}

# Where each lab.yaml key lands in the environment. One table rather than a
# dataclass per section: the shape of lab.yaml is a presentation choice and the
# environment is the contract, so the mapping between them is the thing worth
# being able to read in one screen.
MAPPING: list[tuple[str, str]] = [
    ("student.opensearch_password", "OPENSEARCH_PASSWORD"),
    ("student.bucket_crn", "COS_BUCKET_CRN"),
    ("docling.url", "DOCLING_SERVICE_URL"),
    ("docling.api_key", "DOCLING_SERVICE_API_KEY"),
    ("kafka.bootstrap_servers", "KAFKA_BOOTSTRAP_SERVERS"),
    ("kafka.api_key", "KAFKA_API_KEY"),
    ("kafka.api_secret", "KAFKA_API_SECRET"),
    ("kafka.security_protocol", "KAFKA_SECURITY_PROTOCOL"),
    ("kafka.sasl_mechanism", "KAFKA_SASL_MECHANISM"),
    ("cmf.url", "CMF_URL"),
    ("cmf.environment", "CMF_ENVIRONMENT"),
    ("cmf.auth", "CMF_AUTH"),
    ("cmf.image", "PIPELINE_IMAGE"),
    ("watsonx.url", "WATSONX_URL"),
    ("watsonx.project_id", "WATSONX_PROJECT_ID"),
    ("watsonx.api_key", "WATSONX_APIKEY"),
    ("opensearch.hosts", "OPENSEARCH_HOSTS"),
    ("cos.region", "COS_BUCKET_REGION"),
    ("cos.endpoint", "COS_ENDPOINT"),
]

# The two CA files. Separate from MAPPING because their value is a path that has
# to be resolved against the project root and then proved to exist — a missing
# CA is a TLS handshake failure three steps later, in a component that reports
# it as "connection failed".
CA_FILES: list[tuple[str, str, str]] = [
    ("kafka.ca_file", "KAFKA_CA_LOCATION", "certs/kafka-ca.crt"),
    ("opensearch.ca_file", "OPENSEARCH_CA_LOCATION", "certs/root-ca.pem"),
]

# Required to get anywhere at all, phrased as the lab.yaml key a student reads.
REQUIRED = [
    ("student.id", "your two-digit student id"),
    ("kafka.bootstrap_servers", "the Confluent brokers"),
    ("kafka.api_key", "the Kafka API key"),
    ("kafka.api_secret", "the Kafka API secret"),
    ("cmf.url", "where your Flink job is deployed"),
    ("cmf.auth", "how it authenticates, as user:password"),
    ("cmf.image", "the published pipeline image"),
    # Without it the job deploys, starts, consumes the whole topic and fails on
    # its first embedding call — so it is refused here instead.
    ("watsonx.project_id", "the watsonx.ai project the embedding calls are billed to"),
]


# --- the workbench, which is a consequence of the service URL ----------------
# A Docling SaaS instance is reachable under two names and one id:
#
#     https://api.<region>.dcls.saas.ibm.com/<instance>                    service
#     https://workbench.<region>.dcls.saas.ibm.com/instances/<instance>    workbench
#
# so the second follows from the first, and asking for both only creates the
# chance to hold two halves of two different trials — which reads, from the
# dashboard, as a workbench that never shows the job the pipeline is waiting on.
# Anything that is not a SaaS service URL — a docling-serve on the laptop, say —
# has no workbench, and this says so by returning "".
_SERVICE_HOST = "api."


def docling_workbench_url(service_url: str) -> str:
    """The workbench UI for a Docling SaaS service URL, or ``""``."""
    parts = urlsplit(service_url.strip())
    instance = parts.path.strip("/")
    if not (parts.scheme and parts.netloc and instance):
        return ""
    if not parts.netloc.startswith(_SERVICE_HOST):
        return ""
    host = "workbench." + parts.netloc[len(_SERVICE_HOST):]
    return urlunsplit((parts.scheme, host, f"/instances/{instance}", "", ""))


def _dig(tree: dict, dotted: str):
    """``a.b.c`` out of nested dicts; ``None`` for anything missing."""
    node = tree
    for part in dotted.split("."):
        if not isinstance(node, dict):
            return None
        node = node.get(part)
    return node


@dataclass
class LabConfig:
    """One student's whole configuration, and the environment it produces."""

    raw: dict = field(default_factory=dict)
    path: Path | None = None
    student_id: str = ""
    variant: str = "simple"
    problems: list[str] = field(default_factory=list)
    #: When true, nothing per-student is derived and the names come from the
    #: environment instead. This is the non-workshop deployment, which owns one
    #: pipeline under fixed names rather than thirty under derived ones — so
    #: ``KAFKA_CHUNKS_TOPIC``,
    #: ``APP_NAME`` and the rest are configuration there, not consequences of
    #: an id. It is a flag rather than "let the environment win everywhere"
    #: because in the fan-out the environment is *stale*: ws_student_env
    #: exports student 03's topics, and the next call would inherit them.
    standalone: bool = False

    # --- derived names ------------------------------------------------------
    @property
    def prefix(self) -> str:
        return self._setting("WS_PREFIX")

    @property
    def app_name(self) -> str:
        return f"{self.prefix}-{self.student_id}-{self.variant}"

    @property
    def topics(self) -> dict[str, str]:
        """The five, and both jobs use the same names.

        A student who switches from simple to full keeps reading the same chunks
        and writing the same output topic; the three extra ones are simply
        unused by the simple job.
        """
        p = f"{self.prefix}.{self.student_id}"
        return {
            "KAFKA_CHUNKS_TOPIC": f"{p}.chunks",
            "KAFKA_OUTPUT_TOPIC": f"{p}.enriched",
            "KAFKA_POLICY_TOPIC": f"{p}.policy",
            "KAFKA_QUARANTINE_TOPIC": f"{p}.pii",
            "KAFKA_REJECTED_TOPIC": f"{p}.rejected",
        }

    def variant_topics(self, variant: str | None = None) -> list[str]:
        """The topics a variant needs. The broker does not auto-create, and a
        sink whose topic is missing fails the job at its first record."""
        t = self.topics
        names = [t["KAFKA_CHUNKS_TOPIC"], t["KAFKA_OUTPUT_TOPIC"]]
        if (variant or self.variant) == "full":
            names += [t["KAFKA_POLICY_TOPIC"], t["KAFKA_QUARANTINE_TOPIC"], t["KAFKA_REJECTED_TOPIC"]]
        return names

    @property
    def opensearch_username(self) -> str:
        return f"{self._setting('WS_OS_USER_TEMPLATE')}{self.student_id}"

    @property
    def opensearch_index(self) -> str:
        """Derived, and not a preference.

        The cluster's security plugin grants ``studentNN`` full rights on
        ``studentNN-*`` and nothing anywhere else, so a name outside that prefix
        is a 403 on every write from a pipeline that otherwise looks perfectly
        healthy.
        """
        return f"{self.opensearch_username}-chunks"

    @property
    def bucket_name(self) -> str:
        """What the bucket is called: the name inside the CRN when there is one.

        Bucket names are globally unique across all of IBM Cloud, so a student
        whose suggested name was taken created a different one — and the CRN
        they pasted back is where that shows up. The template is only ever a
        suggestion for a bucket that does not exist yet.
        """
        crn = str(_dig(self.raw, "student.bucket_crn") or "")
        if ":bucket:" in crn:
            return crn.rsplit(":bucket:", 1)[1]
        template = str(_dig(self.raw, "cos.bucket_template") or "")
        return template.replace("{id}", self.student_id) if template else ""

    def _setting(self, key: str) -> str:
        """A default, possibly overridden from the environment.

        Deliberately does not go through :meth:`environ`: the derived names are
        built *from* these, so reading them back out of the result would be a
        cycle.
        """
        return os.environ.get(key) or DEFAULTS.get(key, "")

    # --- the environment ----------------------------------------------------
    def environ(self, variant: str | None = None) -> dict[str, str]:
        """Everything every component reads, resolved.

        Deliberately includes keys whose value is empty. An empty
        ``COS_BUCKET_CRN`` is the statement "this student's bucket is unknown",
        and the dashboard has to be able to tell that apart from "not asked".
        """
        variant = variant or self.variant
        env: dict[str, str] = dict(DEFAULTS)
        env.update(PER_VARIANT[variant])

        for dotted, name in MAPPING:
            value = _dig(self.raw, dotted)
            if value is not None:
                env[name] = "" if value is None else str(value)

        for dotted, name, fallback in CA_FILES:
            value = str(_dig(self.raw, dotted) or fallback)
            env[name] = str((PROJECT_ROOT / value).resolve()) if not os.path.isabs(value) else value

        # The environment wins over the file: this is how the instructor's
        # fan-out feeds the same code path from its own configuration.
        #
        # Except for the per-variant sizing, which is a property of the variant
        # and not a setting. Letting it be overridden here is how a process that
        # resolved `simple` first would then hand the `simple` TaskManager to
        # every `full` deploy it resolved afterwards — the values are exported,
        # so they come straight back in as an override. The bug is silent: the
        # job is admitted and dies later on memory.
        overridable = (set(env) | {n for _, n in MAPPING}) - set(PER_VARIANT[variant])
        for name in overridable:
            if name in os.environ:
                env[name] = os.environ[name]

        if self.standalone:
            # No id, so the names above are configuration here. WS_SINK_TYPE is
            # still the alias of SINK_TYPE — the descriptor substitutes the one,
            # the job reads the other.
            # Every one of them is defined, empty when the environment does not
            # say: a descriptor that asks for OPENSEARCH_INDEX while the sink is
            # kafka-only should render with an empty one, not refuse.
            for name in DERIVED_KEYS:
                env[name] = os.environ.get(name, "")
            env["WS_SINK_TYPE"] = env["WS_SINK_TYPE"] or env["SINK_TYPE"]
            env["WS_VARIANT"] = env["WS_VARIANT"] or variant
            return env

        env.update(self.topics)
        env.update(
            STUDENT_ID=self.student_id,
            WS_ID=self.student_id,
            WS_VARIANT=variant,
            APP_NAME=f"{self.prefix}-{self.student_id}-{variant}",
            JOB_MODULE=env["WS_MODULE_FULL"] if variant == "full" else env["WS_MODULE_SIMPLE"],
            # One group per variant. Not strictly required — these jobs run
            # without checkpointing, so no offset is ever committed and both
            # would replay from earliest anyway — but it keeps the two runs
            # legible in Kafka's own tooling.
            KAFKA_CONSUMER_GROUP=f"{self.prefix}-{self.student_id}-{variant}",
            OPENSEARCH_USERNAME=self.opensearch_username,
            OPENSEARCH_INDEX=self.opensearch_index,
            COS_BUCKET=self.bucket_name,
            COS_BUCKET_CRN=str(_dig(self.raw, "student.bucket_crn") or ""),
            # WS_SINK_TYPE is what the descriptor substitutes; SINK_TYPE is what
            # the job reads. One value, two names, because the template predates
            # the config file and renaming it in the cluster is not free.
            WS_SINK_TYPE=env["SINK_TYPE"],
        )
        return env

    # --- validation ---------------------------------------------------------
    def validate(self) -> list[str]:  # noqa: C901
        """Problems, each naming the ``lab.yaml`` key rather than the variable.

        A student is looking at YAML; telling them ``CMF_AUTH`` is unset sends
        them looking for a file that no longer exists.
        """
        problems: list[str] = list(self.problems)

        if self.standalone:
            # No id to check; what must be present instead is the naming this
            # mode takes from the environment.
            env = self.environ()
            for name in ("APP_NAME", "JOB_MODULE", "KAFKA_CHUNKS_TOPIC", "KAFKA_OUTPUT_TOPIC"):
                if not env.get(name):
                    problems.append(f"{name} is not set — standalone mode takes it from the environment")
            return problems

        raw_id = _dig(self.raw, "student.id")
        if raw_id is None or str(raw_id).strip() == "":
            problems.append("student.id is empty — set your two-digit id (e.g. \"07\")")
        elif not re.fullmatch(r"\d{1,2}", str(raw_id).strip()):
            problems.append(f"student.id must be a number like 07 (got {raw_id!r})")

        for dotted, what in REQUIRED:
            if dotted == "student.id":
                continue
            if not str(_dig(self.raw, dotted) or "").strip():
                problems.append(f"{dotted} is empty — {what}")

        env = self.environ()
        for dotted, name, _fallback in CA_FILES:
            path = Path(env[name])
            if not path.is_file():
                problems.append(
                    f"{dotted}: no file at {path} — the certificate authority that signed "
                    "the brokers. Without it every TLS connection fails as a timeout."
                )

        # A dimension that disagrees with the model is not caught until the
        # first embedding response, by which time the job has consumed the whole
        # topic. ca-tor's four models and their dimensions:
        known = {
            "ibm/granite-embedding-278m-multilingual": 768,
            "ibm/slate-125m-english-rtrvr-v2": 768,
            "ibm/slate-30m-english-rtrvr-v2": 384,
            "intfloat/multilingual-e5-large": 1024,
        }
        model, dim = env["EMBEDDING_MODEL_ID"], env["EMBEDDING_DIMENSION"]
        if model in known and str(known[model]) != str(dim):
            problems.append(
                f"embedding dimension {dim} does not match {model}, which returns {known[model]}"
            )
        return problems

    def require(self) -> LabConfig:
        problems = self.validate()
        if problems:
            where = self.path or DEFAULT_CONFIG
            raise LabConfigError(
                f"{where}:\n" + "\n".join(f"  - {p}" for p in problems)
            )
        return self


def load(path: str | Path | None = None, *, student: str | None = None,
         variant: str | None = None, standalone: bool = False) -> LabConfig:
    """Read ``lab.yaml``.

    A missing file is not an error here: the instructor's scripts drive the
    same code through the environment, and ``validate()`` is
    what decides whether what was resolved is usable.
    """
    cfg = LabConfig(standalone=standalone)
    cfg.path = Path(path) if path else DEFAULT_CONFIG
    if cfg.path.is_file():
        try:
            import yaml
        except ImportError:  # pragma: no cover - only without the local group
            raise LabConfigError("pyyaml is not installed — run `uv sync`") from None
        try:
            loaded = yaml.safe_load(cfg.path.read_text()) or {}
        except yaml.YAMLError as exc:
            raise LabConfigError(f"{cfg.path} is not valid YAML:\n  {exc}") from None
        if not isinstance(loaded, dict):
            raise LabConfigError(f"{cfg.path}: expected a mapping at the top level")
        cfg.raw = loaded

    raw_id = student or os.environ.get("WS_ID") or _dig(cfg.raw, "student.id") or ""
    raw_id = str(raw_id).strip()
    cfg.student_id = f"{int(raw_id):02d}" if re.fullmatch(r"\d{1,2}", raw_id) else raw_id

    chosen = variant or os.environ.get("WS_VARIANT") or "simple"
    if chosen not in VARIANTS:
        raise LabConfigError(f"unknown pipeline {chosen!r} — expected one of {', '.join(VARIANTS)}")
    cfg.variant = chosen
    return cfg


# --- rendering --------------------------------------------------------------
_PLACEHOLDER = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def render(template: str, env: dict[str, str]) -> str:
    """Substitute ``${NAME}`` from ``env``, refusing anything unset.

    This is what replaced ``envsubst``, and the refusal is the whole point:
    ``envsubst`` substitutes an *empty string* for a name it does not know, so a
    typo in the descriptor produced a FlinkApplication that CMF accepted, that
    started, and that then read from a topic called "" forever. It also means
    the lab needs no gettext on the VM.
    """
    missing: list[str] = []

    def one(match: re.Match) -> str:
        name = match.group(1)
        if name not in env:
            missing.append(name)
            return match.group(0)
        return env[name]

    out = _PLACEHOLDER.sub(one, template)
    if missing:
        raise LabConfigError(
            "the descriptor asks for values that are not set: " + ", ".join(sorted(set(missing)))
        )
    return out


# --- CLI --------------------------------------------------------------------
def _export_lines(env: dict[str, str]) -> str:
    return "\n".join(f"export {k}={shlex.quote(v)}" for k, v in sorted(env.items()))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="labtools.config", description=__doc__.split("\n")[0])
    ap.add_argument("action", choices=("env", "render", "check", "show", "topics"))
    ap.add_argument("template", nargs="?", help="for `render`: the descriptor template")
    ap.add_argument("--config", help="path to lab.yaml (default: the project root's)")
    ap.add_argument("--student", help="override student.id — the instructor's fan-out uses this")
    ap.add_argument("--variant", choices=VARIANTS, help="which pipeline (default: simple)")
    ap.add_argument("--standalone", action="store_true",
                    help="do not derive per-student names — take them from the environment")
    args = ap.parse_args(argv)

    try:
        cfg = load(args.config, student=args.student, variant=args.variant,
                   standalone=args.standalone)
        if args.action == "check":
            problems = cfg.validate()
            for p in problems:
                print(f"  - {p}", file=sys.stderr)
            return 1 if problems else 0
        if args.action == "topics":
            print("\n".join(cfg.variant_topics()))
            return 0
        cfg.require()
        env = cfg.environ()
        if args.action == "env":
            print(_export_lines(env))
        elif args.action == "show":
            masked = {
                k: ("***" if any(s in k for s in ("SECRET", "PASSWORD", "APIKEY", "API_KEY", "AUTH")) and v else v)
                for k, v in sorted(env.items())
            }
            print(json.dumps(masked, indent=2))
        elif args.action == "render":
            if not args.template:
                ap.error("render needs a template path")
            print(render(Path(args.template).read_text(), env), end="")
    except LabConfigError as exc:
        print(f"lab.yaml: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
