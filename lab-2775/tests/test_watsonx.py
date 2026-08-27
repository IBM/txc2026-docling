"""Unit tests for the watsonx.ai embedding client.

No network: a stub session records what would have been sent and replays
canned responses. The point is the behaviour that is easy to get wrong and
expensive to discover in a running job — batching, token refresh, retries, and
the dimension check.
"""

from __future__ import annotations

import pytest

from pipeline.config import EmbeddingConfig, WatsonxConfig
from pipeline.watsonx import WatsonxEmbeddings, WatsonxError, batched, unit


class FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None, text=""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._payload


class FakeSession:
    """Stands in for requests.Session; replays queued responses in order."""

    def __init__(self, embed_responses, token_response=None):
        self._embed = list(embed_responses)
        self._token = token_response or FakeResponse(200, {"access_token": "tok-1", "expires_in": 3600})
        self.embed_calls: list[dict] = []
        self.token_calls = 0
        self.closed = False

    def post(self, url, data=None, json=None, headers=None, params=None, timeout=None):
        if "identity/token" in url:
            self.token_calls += 1
            return self._token
        self.embed_calls.append({"url": url, "body": json, "headers": headers, "params": params})
        return self._embed.pop(0)

    def close(self):
        self.closed = True


def make_client(session, dimension=3, normalize=False, **cfg_kwargs):
    watsonx = WatsonxConfig(
        api_key="key", project_id="proj", space_id="", max_retries=cfg_kwargs.pop("max_retries", 2),
        retry_base_s=0.0, **cfg_kwargs
    )
    embedding = EmbeddingConfig(model_id="ibm/test-model", dimension=dimension, normalize=normalize)
    client = WatsonxEmbeddings(watsonx, embedding)
    client._session = session
    return client


def embed_ok(vectors):
    return FakeResponse(200, {"results": [{"embedding": v} for v in vectors]})


# --- helpers ---------------------------------------------------------------


def test_unit_scales_to_length_one():
    assert unit([3.0, 4.0]) == [0.6, 0.8]


def test_unit_leaves_zero_vector_alone():
    assert unit([0.0, 0.0]) == [0.0, 0.0]


def test_batched_splits_without_empty_slices():
    assert batched(["a", "b", "c"], 2) == [["a", "b"], ["c"]]
    assert batched([], 2) == []


# --- batching --------------------------------------------------------------


def test_one_request_per_batch_and_order_preserved():
    session = FakeSession([embed_ok([[1, 0, 0], [0, 1, 0]]), embed_ok([[0, 0, 1]])])
    client = make_client(session, max_batch=2)
    vectors = client.embed(["a", "b", "c"])

    assert vectors == [[1, 0, 0], [0, 1, 0], [0, 0, 1]]
    assert len(session.embed_calls) == 2, "should be two HTTP calls, not three"
    assert session.embed_calls[0]["body"]["inputs"] == ["a", "b"]
    assert session.embed_calls[1]["body"]["inputs"] == ["c"]


def test_empty_input_makes_no_request():
    session = FakeSession([])
    assert make_client(session).embed([]) == []
    assert session.embed_calls == []


def test_project_id_is_sent_and_space_id_is_not():
    session = FakeSession([embed_ok([[1, 0, 0]])])
    make_client(session).embed(["a"])
    body = session.embed_calls[0]["body"]
    assert body["project_id"] == "proj"
    assert "space_id" not in body
    assert body["model_id"] == "ibm/test-model"


def test_input_truncation_is_requested_at_the_configured_budget():
    """A chunk sized to a 512-token budget by one tokenizer can measure 516 to
    the service's, and without this parameter that is a 400 for the whole batch
    and a job that dies on one over-long chunk."""
    session = FakeSession([embed_ok([[1, 0, 0]])])
    embedding = EmbeddingConfig(model_id="ibm/test-model", dimension=3, max_tokens=512)
    client = WatsonxEmbeddings(
        WatsonxConfig(api_key="key", project_id="proj", space_id="", retry_base_s=0.0), embedding
    )
    client._session = session
    client.embed(["a"])
    assert session.embed_calls[0]["body"]["parameters"] == {"truncate_input_tokens": 512}


def test_normalisation_is_applied_when_asked():
    session = FakeSession([embed_ok([[0.0, 3.0, 4.0]])])
    client = make_client(session, normalize=True)
    assert client.embed(["a"]) == [[0.0, 0.6, 0.8]]


# --- the dimension check ---------------------------------------------------


def test_wrong_dimension_fails_immediately():
    session = FakeSession([embed_ok([[1, 0]])])
    client = make_client(session, dimension=384)
    with pytest.raises(WatsonxError, match="384"):
        client.embed(["a"])


def test_dimension_checked_only_once():
    session = FakeSession([embed_ok([[1, 0, 0]]), embed_ok([[0, 1, 0]])])
    client = make_client(session, max_batch=1)
    client.embed(["a", "b"])
    assert client._checked_dimension is True


# --- auth and retries ------------------------------------------------------


def test_token_is_fetched_once_and_reused():
    session = FakeSession([embed_ok([[1, 0, 0]]), embed_ok([[0, 1, 0]])])
    client = make_client(session, max_batch=1)
    client.embed(["a", "b"])
    assert session.token_calls == 1
    assert session.embed_calls[0]["headers"]["Authorization"] == "Bearer tok-1"


def test_401_refreshes_the_token_and_retries():
    session = FakeSession([FakeResponse(401, text="expired"), embed_ok([[1, 0, 0]])])
    client = make_client(session)
    assert client.embed(["a"]) == [[1, 0, 0]]
    assert session.token_calls == 2, "should have re-fetched the token"


def test_429_is_retried():
    session = FakeSession([FakeResponse(429, text="slow down"), embed_ok([[1, 0, 0]])])
    client = make_client(session)
    assert client.embed(["a"]) == [[1, 0, 0]]
    assert len(session.embed_calls) == 2


def test_retries_are_bounded_then_raise():
    session = FakeSession([FakeResponse(503, text="down")] * 5)
    client = make_client(session, max_retries=2)
    with pytest.raises(WatsonxError, match="503"):
        client.embed(["a"])
    assert len(session.embed_calls) == 3, "initial attempt plus two retries"


def test_400_is_not_retried():
    session = FakeSession([FakeResponse(400, text="bad model_id"), embed_ok([[1, 0, 0]])])
    client = make_client(session)
    with pytest.raises(WatsonxError, match="bad model_id"):
        client.embed(["a"])
    assert len(session.embed_calls) == 1


def test_result_count_mismatch_is_an_error():
    session = FakeSession([embed_ok([[1, 0, 0]])])
    client = make_client(session)
    with pytest.raises(WatsonxError, match="results"):
        client.embed(["a", "b"])


def test_embed_before_open_is_an_error():
    client = WatsonxEmbeddings(WatsonxConfig(api_key="k", project_id="p"), EmbeddingConfig())
    with pytest.raises(WatsonxError, match="open"):
        client.embed(["a"])


# --- configuration validation ----------------------------------------------


def test_require_rejects_a_missing_api_key():
    with pytest.raises(RuntimeError, match="WATSONX_APIKEY"):
        WatsonxConfig(api_key="", project_id="p").require()


def test_require_rejects_a_missing_scope():
    with pytest.raises(RuntimeError, match="WATSONX_PROJECT_ID"):
        WatsonxConfig(api_key="k", project_id="", space_id="").require()


def test_require_rejects_both_scopes():
    with pytest.raises(RuntimeError, match="exactly one"):
        WatsonxConfig(api_key="k", project_id="p", space_id="s").require()


def test_require_accepts_space_id_alone():
    WatsonxConfig(api_key="k", project_id="", space_id="s").require()
