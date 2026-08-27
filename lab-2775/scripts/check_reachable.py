#!/usr/bin/env python3
"""Can this machine reach the four systems the lab needs? — ``./setup.sh check``.

Everything here is a *read*. Nothing is created, nothing is deployed, and every
probe is allowed to fail: the point is to turn four different failure modes into
four lines on one screen, before a student spends twenty minutes looking at a
green pipeline that produces nothing.

Each of these has cost a rehearsal an afternoon:

* a broker CA that is present but wrong — the connection times out rather than
  reporting a certificate error, so it reads as "the cluster is down";
* CMF credentials that authenticate but against an environment that does not
  exist — every deploy is a 404, one per student;
* an OpenSearch account that can log in but cannot write ``studentNN-*``, which
  is a 403 on every record from a job that reports itself RUNNING;
* a watsonx model id the region does not serve, which is a 404 on the first
  batch and not a fallback to something else.
"""

from __future__ import annotations

import os
import sys

OK, BAD, SKIP = "  ✓", "  ✗", "  ·"


def line(mark: str, what: str, detail: str = "") -> None:
    print(f"{mark} {what:<14} {detail}")


def brief(exc: Exception) -> str:
    """One readable clause, not a urllib3 traceback in prose.

    A student reading four lines needs to know *which* of them failed and
    roughly why; the full chain — ConnectionError wrapping MaxRetryError
    wrapping NameResolutionError — is three lines of noise per probe.
    """
    text = str(exc)
    if "Failed to resolve" in text or "NameResolutionError" in text or "nodename nor servname" in text:
        return "cannot resolve that host — check the address"
    if "timed out" in text.lower():
        return "timed out — check the address, or whether you are on the lab network"
    if "certificate" in text.lower():
        return "TLS refused the certificate — check the ca_file"
    if "_TRANSPORT" in text or "Broker transport failure" in text:
        return "cannot reach the brokers — check the address, the CA file and the network"
    if "_AUTHENTICATION" in text or "authentication" in text.lower():
        return "the brokers refused the credentials — check kafka.api_key / api_secret"
    if "Connection refused" in text:
        return "connection refused — nothing is listening there"
    return text.split("\n")[0][:120]


def check_kafka() -> bool:
    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
    if not servers:
        line(SKIP, "Kafka", "kafka.bootstrap_servers is empty")
        return False
    try:
        from confluent_kafka.admin import AdminClient

        from labtools.kafka import client_config

        # log_level 0 keeps librdkafka's own connection failures off stderr:
        # it prints them per broker per retry, which buries the four lines this
        # script exists to print. The exception below says the same thing once.
        md = AdminClient({**client_config(), "log_level": 0}).list_topics(timeout=15)
    except Exception as exc:  # noqa: BLE001
        line(BAD, "Kafka", f"{servers} — {brief(exc)}")
        return False
    mine = sorted(t for t in md.topics if t.startswith(f"ws.{os.environ.get('STUDENT_ID', '')}."))
    line(OK, "Kafka", f"{len(md.brokers)} broker(s); {len(mine)} of your topics exist")
    return True


def check_cmf() -> bool:
    url, env = os.environ.get("CMF_URL", ""), os.environ.get("CMF_ENVIRONMENT", "")
    auth = os.environ.get("CMF_AUTH", "")
    if not (url and auth):
        line(SKIP, "Flink (CMF)", "cmf.url or cmf.auth is empty")
        return False
    import requests

    user, _, password = auth.partition(":")
    try:
        # verify=False for the same reason every curl against CMF carries -k:
        # the ingress presents a certificate for an internal name.
        r = requests.get(
            f"{url}/api/v1/environments/{env}/applications",
            auth=(user, password), timeout=15, verify=False,
        )
    except Exception as exc:  # noqa: BLE001
        line(BAD, "Flink (CMF)", f"{url} — {brief(exc)}")
        return False
    if r.status_code == 401:
        line(BAD, "Flink (CMF)", "authentication refused — check cmf.auth")
        return False
    if r.status_code == 404:
        line(BAD, "Flink (CMF)", f"no environment named {env!r} — check cmf.environment")
        return False
    if r.status_code >= 400:
        line(BAD, "Flink (CMF)", f"HTTP {r.status_code}")
        return False
    apps = (r.json() or {}).get("items", []) if r.text.strip().startswith("{") else []
    line(OK, "Flink (CMF)", f"environment {env!r} reachable, {len(apps)} application(s) in it")
    return True


def check_opensearch() -> bool:
    hosts = os.environ.get("OPENSEARCH_HOSTS", "")
    user = os.environ.get("OPENSEARCH_USERNAME", "")
    password = os.environ.get("OPENSEARCH_PASSWORD", "")
    if not hosts:
        line(SKIP, "OpenSearch", "opensearch.hosts is empty")
        return False
    if not password:
        line(SKIP, "OpenSearch", "student.opensearch_password is empty — the Ask tab needs it")
        return False
    import requests

    index = os.environ.get("OPENSEARCH_INDEX", "")
    try:
        r = requests.get(
            f"{hosts.rstrip('/')}/{index}",
            auth=(user, password), timeout=15,
            verify=os.environ.get("OPENSEARCH_CA_LOCATION") or True,
        )
    except Exception as exc:  # noqa: BLE001
        line(BAD, "OpenSearch", f"{hosts} — {brief(exc)}")
        return False
    if r.status_code == 401:
        line(BAD, "OpenSearch", f"{user}: wrong password")
        return False
    if r.status_code == 403:
        # The security plugin grants studentNN rights on studentNN-* and
        # nothing anywhere else, so this means the index name is outside it.
        line(BAD, "OpenSearch", f"{user} may not read {index!r}")
        return False
    if r.status_code == 404:
        line(OK, "OpenSearch", f"reachable as {user}; {index} not created yet (./setup.sh index)")
        return True
    line(OK, "OpenSearch", f"reachable as {user}; {index} exists")
    return True


def check_watsonx() -> bool:
    """The model id and the region, which are chosen together.

    Not an embedding call: that needs the API key, and a student never has one
    — the deployed job reads it from a Kubernetes Secret. What is checkable
    from here is whether the region serves the model at all, which is the
    mistake that produces a 404 on the job's first batch.
    """
    url = os.environ.get("WATSONX_URL", "")
    model = os.environ.get("EMBEDDING_MODEL_ID", "")
    if not url:
        line(SKIP, "watsonx.ai", "watsonx.url is empty")
        return False
    import requests

    try:
        r = requests.get(
            f"{url.rstrip('/')}/ml/v1/foundation_model_specs",
            params={"version": "2024-05-01", "filters": "function_embedding"}, timeout=15,
        )
        served = {m.get("model_id") for m in (r.json() or {}).get("resources", [])}
    except Exception as exc:  # noqa: BLE001
        line(BAD, "watsonx.ai", f"{url} — {brief(exc)}")
        return False
    if not served:
        line(SKIP, "watsonx.ai", f"{url} answered, but published no catalogue")
        return True
    if model in served:
        line(OK, "watsonx.ai", f"{url.split('//')[-1].split('.')[0]} serves {model}")
        return True
    line(BAD, "watsonx.ai", f"this region does not serve {model} — it serves: {', '.join(sorted(served))}")
    return False


def main() -> int:
    print("Reachable from here")
    results = [check_kafka(), check_cmf(), check_opensearch(), check_watsonx()]
    if not all(results):
        print("\n  Some of that is not ready. Every line above is independent —")
        print("  a ✗ on OpenSearch does not stop you deploying a pipeline.")
    return 0


if __name__ == "__main__":
    import warnings

    # verify=False against CMF is deliberate (see above); the warning would fire
    # once per probe and bury the four lines this script exists to print.
    warnings.filterwarnings("ignore", message="Unverified HTTPS request")
    sys.exit(main())
