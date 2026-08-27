"""The watsonx.ai clients: embeddings for the pipeline, chat for the RAG tab.

The vectors used to come from a `sentence-transformers` model loaded into every
Python worker. That cost 883 MiB of resident memory per worker and dragged
torch (720 MB), scipy, scikit-learn and a 1.2 GB model snapshot into the image;
on a shared cluster it is what made a class of thirty impossible. Calling
watsonx.ai instead leaves `requests` as the only thing the embed stage needs.

pyflink-free on purpose (see CLAUDE.md): the operators and the laptop-side
scripts share this client, so the two cannot drift apart, and it is unit
testable without a Flink runtime.

Two things this module insists on, because both fail silently otherwise:

* **Batching.** One HTTP round trip per chunk is 1000 requests per student.
  ``embed`` slices its input into ``max_batch`` and sends whole batches, which
  is also what the API is shaped for.
* **The dimension.** A model whose vectors are not ``EMBEDDING_DIMENSION`` long
  does not fail at the sink — it fails at the OpenSearch ``knn_vector`` mapping,
  much later and much less legibly. The first response is checked and the job
  dies immediately if it disagrees.

:class:`WatsonxChat` is here for the same reason the embeddings client is: the
inspector's Ask tab (``dashboard/inspector/rag.py``) has to authenticate to
watsonx.ai exactly as the operators do — the same IAM exchange, the same
refresh-on-401, the same retry set — and a second copy of that would be a
second thing to keep right. No Flink job calls it; the retrieval half of RAG is
what the *pipeline* builds, and the answering half is what reads the index back.
"""

from __future__ import annotations

import logging
import math
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .config import EmbeddingConfig, GenerationConfig, WatsonxConfig

logger = logging.getLogger(__name__)

# Retried: 429 is the rate limit thirty students share, 5xx is the service.
_RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
# Refresh the IAM token this long before it actually expires.
_TOKEN_SKEW_S = 60.0


class WatsonxError(RuntimeError):
    """Anything the embeddings API refused that a retry will not fix."""


def unit(vector: list[float]) -> list[float]:
    """Scale to length 1, leaving a zero vector alone.

    Done here rather than asked of the API: watsonx.ai does not promise unit
    vectors, and the OpenSearch mapping scores with cosine similarity.
    """
    norm = math.sqrt(sum(v * v for v in vector))
    if norm == 0.0:
        return list(vector)
    return [v / norm for v in vector]


def batched(items: list[str], size: int) -> list[list[str]]:
    """Split into slices of at most ``size`` (never an empty slice)."""
    if size < 1:
        raise ValueError("batch size must be >= 1")
    return [items[i : i + size] for i in range(0, len(items), size)]


class _WatsonxSession:
    """The half of a watsonx.ai client that has nothing to do with the model:
    an HTTP session, an IAM token that refreshes itself, and a POST that knows
    which failures are worth retrying.

    Both clients below are built in their caller's ``open()``, never in
    ``__init__`` — for the operators, because the session and the token must
    not be pickled into the job graph; for the dashboard, because Streamlit
    reruns the script on every interaction and a client built at import would
    be rebuilt with it.
    """

    def __init__(self, watsonx: WatsonxConfig) -> None:
        self._cfg = watsonx
        self._session = None
        self._token: str | None = None
        self._token_expiry = 0.0

    # --- lifecycle ---------------------------------------------------------

    def open(self) -> None:
        import requests

        self._cfg.require()
        self._session = requests.Session()

    def close(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    # --- IAM ---------------------------------------------------------------

    def _bearer(self, force: bool = False) -> str:
        """Cached IAM token. They last an hour, so a job that runs for a day
        must refresh — the 401-and-retry path in ``_post`` covers the case
        where the clock and the service disagree about when that is."""
        now = time.monotonic()
        if not force and self._token and now < self._token_expiry:
            return self._token

        response = self._session.post(
            self._cfg.iam_url,
            data={
                "grant_type": "urn:ibm:params:oauth:grant-type:apikey",
                "apikey": self._cfg.api_key,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=self._cfg.timeout_s,
        )
        if response.status_code != 200:
            raise WatsonxError(f"IAM token request failed: HTTP {response.status_code} {response.text[:300]}")
        payload = response.json()
        self._token = payload["access_token"]
        self._token_expiry = now + max(float(payload.get("expires_in", 3600)) - _TOKEN_SKEW_S, 0.0)
        return self._token

    # --- the request ------------------------------------------------------

    def _post(self, path: str, body: dict) -> dict:
        """POST ``body`` to a watsonx.ai path, retrying what is worth retrying."""
        if self._session is None:
            raise WatsonxError(f"{type(self).__name__}.open() was never called")
        url = f"{self._cfg.url.rstrip('/')}/{path.lstrip('/')}"
        params = {"version": self._cfg.api_version}
        last: str = "no attempt made"

        for attempt in range(self._cfg.max_retries + 1):
            response = self._session.post(
                url,
                params=params,
                json=body,
                headers={
                    "Authorization": f"Bearer {self._bearer()}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                timeout=self._cfg.timeout_s,
            )
            if response.status_code == 200:
                return response.json()

            # A token that expired between the cache check and the call.
            if response.status_code == 401 and attempt < self._cfg.max_retries:
                self._bearer(force=True)
                continue

            last = f"HTTP {response.status_code} {response.text[:300]}"
            if response.status_code in _RETRY_STATUS and attempt < self._cfg.max_retries:
                delay = self._backoff(response, attempt)
                logger.warning("watsonx %s %s — retrying in %.1fs (attempt %d)", path, last, delay, attempt + 1)
                time.sleep(delay)
                continue

            raise WatsonxError(f"watsonx {path} failed: {last}")

        raise WatsonxError(f"watsonx {path} failed after {self._cfg.max_retries} retries: {last}")

    def _backoff(self, response, attempt: int) -> float:
        """Honour Retry-After when the service sends one, else exponential."""
        header = response.headers.get("Retry-After") if hasattr(response, "headers") else None
        if header:
            try:
                return min(float(header), 30.0)
            except ValueError:
                pass
        return min(self._cfg.retry_base_s * (2**attempt), 30.0)

    # --- the body of the request, which is where the two clients differ ----
    def _container(self, body: dict) -> dict:
        """Stamp the project or space on a request body.

        The API takes exactly one of the two, and ``WatsonxConfig.require``
        has already refused a configuration that names both or neither.
        """
        if self._cfg.project_id:
            body["project_id"] = self._cfg.project_id
        else:
            body["space_id"] = self._cfg.space_id
        return body


class WatsonxEmbeddings(_WatsonxSession):
    """A batching, token-refreshing client for ``POST /ml/v1/text/embeddings``."""

    PATH = "ml/v1/text/embeddings"

    def __init__(self, watsonx: WatsonxConfig, embedding: EmbeddingConfig) -> None:
        super().__init__(watsonx)
        self._model_id = embedding.model_id
        self._dimension = embedding.dimension
        self._truncate_tokens = embedding.max_tokens
        self._normalize = embedding.normalize
        self._checked_dimension = False

    def open(self) -> None:
        super().open()
        logger.info(
            "watsonx embeddings: model=%s dim=%d batch=%d endpoint=%s",
            self._model_id,
            self._dimension,
            self._cfg.max_batch,
            self._cfg.url,
        )

    def embed(self, texts: list[str]) -> list[list[float]]:
        """Vectors for ``texts``, in the same order, one HTTP call per batch."""
        if self._session is None:
            raise WatsonxError("WatsonxEmbeddings.open() was never called")
        if not texts:
            return []

        vectors: list[list[float]] = []
        for batch in batched(texts, self._cfg.max_batch):
            vectors.extend(self._embed_one_batch(batch))
        return vectors

    def _embed_one_batch(self, batch: list[str]) -> list[list[float]]:
        # truncate_input_tokens is not belt-and-braces: the producer's chunker
        # and the service do not count tokens with the same tokenizer, so a
        # chunk sized to a 512-token budget arrives as 516 often enough to
        # matter. Without this the service answers 400 for the whole batch,
        # WatsonxError propagates, and the job dies on one over-long chunk —
        # which is a worse outcome than losing the tail of it.
        body = self._container({
            "model_id": self._model_id,
            "inputs": batch,
            "parameters": {"truncate_input_tokens": self._truncate_tokens},
        })
        payload = self._post(self.PATH, body)
        results = payload.get("results")
        if not isinstance(results, list) or len(results) != len(batch):
            raise WatsonxError(
                f"watsonx returned {len(results) if isinstance(results, list) else 'no'} "
                f"results for {len(batch)} inputs"
            )

        vectors = [list(item["embedding"]) for item in results]
        self._check_dimension(vectors[0])
        return [unit(v) for v in vectors] if self._normalize else vectors

    def _check_dimension(self, vector: list[float]) -> None:
        """Once, on the first response. A mismatch here is a configuration
        error that would otherwise surface as a rejected OpenSearch bulk write
        with no obvious connection to the model id."""
        if self._checked_dimension:
            return
        if len(vector) != self._dimension:
            raise WatsonxError(
                f"model {self._model_id!r} returns {len(vector)}-dimensional vectors "
                f"but EMBEDDING_DIMENSION is {self._dimension} — set them to agree, and "
                f"rebuild the OpenSearch index if it already exists"
            )
        self._checked_dimension = True


class WatsonxChat(_WatsonxSession):
    """``POST /ml/v1/text/chat`` — the model that answers, for the Ask tab.

    Deliberately thin. It takes messages and returns the assistant's text,
    because that is all a retrieval-augmented answer needs: the retrieval is
    OpenSearch's job and the prompt is :mod:`dashboard.inspector.rag`'s, so
    nothing here knows what a chunk is.

    ``model_id`` is regional in exactly the way ``EMBEDDING_MODEL_ID`` is —
    ca-tor serves two chat models and a third is a 404 on the first question.
    :class:`pipeline.config.GenerationConfig` carries the list.
    """

    PATH = "ml/v1/text/chat"

    def __init__(self, watsonx: WatsonxConfig, generation: GenerationConfig) -> None:
        super().__init__(watsonx)
        self._model_id = generation.model_id
        self._max_tokens = generation.max_tokens
        self._temperature = generation.temperature

    @property
    def model_id(self) -> str:
        return self._model_id

    def open(self) -> None:
        super().open()
        logger.info(
            "watsonx chat: model=%s max_tokens=%d endpoint=%s",
            self._model_id, self._max_tokens, self._cfg.url,
        )

    def chat(self, messages: list[dict], *, max_tokens: int | None = None) -> str:
        """The assistant's reply to ``messages``, as plain text.

        ``messages`` is the OpenAI-shaped list the API takes:
        ``[{"role": "system"|"user"|"assistant", "content": "..."}]``.
        """
        if not messages:
            raise WatsonxError("chat() needs at least one message")

        body = self._container({
            "model_id": self._model_id,
            "messages": messages,
            "max_tokens": max_tokens or self._max_tokens,
            "temperature": self._temperature,
        })
        payload = self._post(self.PATH, body)

        choices = payload.get("choices")
        if not isinstance(choices, list) or not choices:
            raise WatsonxError(f"watsonx chat returned no choices: {str(payload)[:300]}")
        content = choices[0].get("message", {}).get("content")
        if not isinstance(content, str):
            raise WatsonxError(f"watsonx chat returned no message content: {str(choices[0])[:300]}")

        # A model that ran out of budget mid-sentence is worth saying out loud:
        # the answer looks merely abrupt, and the obvious conclusion — that the
        # retrieved context was too thin — is the wrong one.
        if choices[0].get("finish_reason") == "length":
            logger.info("watsonx chat hit max_tokens=%d; the answer is truncated", max_tokens or self._max_tokens)
        return content
