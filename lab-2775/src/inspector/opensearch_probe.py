"""The other end of the pipeline: what actually landed in the index.

Relevant whenever ``SINK_TYPE`` names ``opensearch`` — which in the workshop it
does, alongside ``kafka``: the finished record goes to the output topic *and*
into the index, so the Messages tab and this one show the same records from the
two ends. Every function here degrades to "not reachable" rather than failing,
exactly as the pipeline itself does, because during bring-up the cluster
usually is not.

**A student is not an admin here.** The workshop cluster grants ``studentNN``
full rights on ``studentNN-*`` and nothing else, so anything phrased over all
indices — ``_cat/indices``, ``get_alias("*")`` — comes back 403 for everyone
except the instructor. Cluster-wide questions are therefore asked *optionally*:
a 403 means "not yours to see", which is not an error and must not blank the
page. Where a listing is still useful it is asked for over the student's own
prefix instead.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class IndexView:
    reachable: bool = False
    index: str = ""
    exists: bool = False
    doc_count: int = 0
    size_bytes: int = 0
    dimension: int | None = None
    error: str = ""
    indices: list[str] = field(default_factory=list)


def _client(hosts: str, username: str, password: str, verify: bool, ca_certs: str = ""):
    from opensearchpy import OpenSearch

    kwargs: dict = {
        "hosts": [h.strip() for h in hosts.split(",") if h.strip()],
        "verify_certs": verify,
        "ssl_show_warn": verify,
        "timeout": 6,
    }
    # The workshop cluster's certificate is from a private CA — the same story
    # as the Kafka brokers'. Without this, verification fails and the tab reads
    # "no OpenSearch here" when the truth is "I do not trust this certificate".
    if ca_certs:
        kwargs["ca_certs"] = ca_certs
    if username and password:
        kwargs["http_auth"] = (username, password)
    return OpenSearch(**kwargs)


def connect(hosts: str, username: str = "", password: str = "", verify: bool = False,
            ca_certs: str = ""):
    if not hosts:
        return None
    try:
        client = _client(hosts, username, password, verify, ca_certs)
        client.info()
        return client
    except Exception:  # noqa: BLE001 — "no OpenSearch here" is a normal state
        return None


def why_unreachable(hosts: str, username: str = "", password: str = "", verify: bool = False,
                    ca_certs: str = "") -> str:
    """The reason ``connect`` returned ``None``, for the page to show.

    Separate from ``connect`` because the three causes want three different
    sentences — a wrong password, an untrusted certificate and an unreachable
    host all look identical as "no client", and each has a different fix.
    """
    if not hosts:
        return "OPENSEARCH_HOSTS is not set."
    try:
        _client(hosts, username, password, verify, ca_certs).info()
    except Exception as exc:  # noqa: BLE001
        text = str(exc)
        if "AuthenticationException" in type(exc).__name__ or "401" in text:
            return f"`{username or '(no user)'}` was refused: check OPENSEARCH_USERNAME / OPENSEARCH_PASSWORD."
        if "CERTIFICATE_VERIFY_FAILED" in text or "SSLError" in type(exc).__name__:
            return (
                "The certificate was not trusted. This cluster uses a private CA — point "
                "`OPENSEARCH_CA_LOCATION` at `opensearch/root-ca.pem`, or set "
                "`OPENSEARCH_VERIFY_CERTS=false`."
            )
        return text[:300]
    return ""


def index_view(client, index: str) -> IndexView:
    view = IndexView(reachable=client is not None, index=index)
    if client is None:
        return view
    # Asked over the student's own prefix, not over `*`: `*` is a 403 for every
    # account but the instructor's, and a 403 here would take the whole panel
    # down over a question nobody needed answered.
    try:
        prefix = index.split("-")[0]
        view.indices = sorted(
            n for n in client.indices.get_alias(index=f"{prefix}-*") if not n.startswith(".")
        )
    except Exception:  # noqa: BLE001 — not yours to list is not an error
        view.indices = []
    try:
        view.exists = bool(client.indices.exists(index=index))
        if not view.exists:
            return view
        view.doc_count = int(client.count(index=index)["count"])
        stats = client.indices.stats(index=index)["indices"][index]["total"]
        view.size_bytes = int(stats["store"]["size_in_bytes"])
        mapping = client.indices.get_mapping(index=index)[index]["mappings"]["properties"]
        embedding = mapping.get("embedding", {})
        view.dimension = embedding.get("dimension")
    except Exception as exc:  # noqa: BLE001
        view.error = str(exc)
    return view


def recent(client, index: str, size: int = 25, query: str = "") -> list[dict]:
    """Documents from the index — newest by ``ingested_at`` unless searching."""
    if client is None:
        return []
    body: dict = {"size": size, "_source": {"excludes": ["embedding"]}}
    if query:
        body["query"] = {"multi_match": {"query": query, "fields": ["text", "headings", "heading_path"]}}
    else:
        body["query"] = {"match_all": {}}
        body["sort"] = [{"ingested_at": {"order": "desc", "unmapped_type": "date"}}]
    try:
        hits = client.search(index=index, body=body)["hits"]["hits"]
    except Exception:  # noqa: BLE001
        return []
    return [{"_id": h["_id"], "_score": h.get("_score"), **h["_source"]} for h in hits]
