"""The other end of the lab: asking the index a question.

Everything else in this dashboard *watches* the pipeline. This reads what the
pipeline produced and does the one thing the vectors were for — retrieval-
augmented generation, in the smallest honest form:

    question -> embed (watsonx.ai)          the same model the embed stage used
             -> k-NN over the chunk index   the vectors the sink wrote
             -> prompt with those chunks    numbered, so the answer can cite
             -> answer (watsonx.ai chat)    the same credentials, a chat model

There is no vector store, no framework and no chain: the index is the store,
because the pipeline already built it, and a RAG "chain" here is four function
calls. That is the point worth making at the end of a pipeline workshop — the
hard part was getting good chunks with good vectors into a good index, and this
file is what is left once that is done.

**The query has to be embedded by the model that embedded the chunks.** A
vector from a different model is not wrong in a way that shows: the k-NN search
still returns its k nearest neighbours, they are just unrelated to the question.
So ``EMBEDDING_MODEL_ID`` is read from the same environment the pipeline ran
with, and :func:`answer` checks the query vector against the index's mapped
dimension before it searches — the one mismatch that is cheap to catch.

pyflink-free and streamlit-free on purpose: everything here is a function of its
arguments, so ``tests/test_rag.py`` exercises the prompt and the search body
without a cluster, a model, or a browser.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from pipeline.config import EmbeddingConfig, GenerationConfig, WatsonxConfig
from pipeline.watsonx import WatsonxChat, WatsonxEmbeddings

# Fields the answer never needs and the browser should not have to carry: a
# 768-float vector per hit is most of the response body.
_EXCLUDED_SOURCE = ["embedding"]

# What the model is allowed to do with the passages, and — the half that
# matters in a workshop — what it must do when they do not answer the question.
# A RAG demo that quietly falls back on what the model already knows teaches the
# opposite of what it is there to teach: the whole claim is that the answer came
# out of *these documents*, and the way to show that is to watch it refuse when
# they do not contain one.
SYSTEM_PROMPT = (
    "You answer questions using only the numbered passages provided by the user. "
    "Follow these rules exactly:\n"
    "1. Use only what the passages say. Do not add facts from your own knowledge.\n"
    "2. Cite the passages you used inline, as [1], [2] — every claim gets a citation.\n"
    "3. If the passages do not contain the answer, say so plainly and stop. Do not guess.\n"
    "4. Be concise: a few sentences unless the question needs more."
)


@dataclass(frozen=True)
class Passage:
    """One retrieved chunk, in the shape the prompt and the UI both want."""

    n: int                      # 1-based, and it is what the answer cites
    chunk_id: str
    score: float
    text: str
    filename: str = ""
    doc_id: str = ""
    chunk_index: int | None = None
    headings: list[str] = field(default_factory=list)
    page_numbers: list[int] = field(default_factory=list)

    @property
    def where(self) -> str:
        """A one-line provenance: file, section, page — whatever exists.

        ``headings`` and ``page_numbers`` are opt-in on Docling's target, so a
        chunk may legitimately have neither (see CLAUDE.md) — hence the joins
        rather than an index.
        """
        bits = [self.filename or self.doc_id[:12] or "?"]
        if self.headings:
            bits.append(" › ".join(self.headings[-2:]))
        if self.page_numbers:
            pages = sorted(set(self.page_numbers))
            bits.append(f"p. {pages[0]}" if len(pages) == 1 else f"pp. {pages[0]}–{pages[-1]}")
        return " · ".join(bits)


@dataclass
class Answer:
    """What :func:`answer` produced, including the reasons it produced nothing."""

    text: str = ""
    passages: list[Passage] = field(default_factory=list)
    model_id: str = ""
    error: str = ""

    @property
    def ok(self) -> bool:
        return bool(self.text) and not self.error

    @property
    def cited(self) -> set[int]:
        """The passage numbers the answer actually cited.

        Worth surfacing rather than assuming: retrieval returning ten passages
        and the answer using two is the normal case, and seeing which two is
        most of what makes a RAG demo legible.
        """
        return cited_numbers(self.text)


def cited_numbers(text: str) -> set[int]:
    """Every ``[n]`` in ``text``, as integers. ``[1, 3]`` and ``[1][3]`` both count."""
    numbers: set[int] = set()
    for group in re.findall(r"\[([\d,\s]+)\]", text):
        for part in group.split(","):
            part = part.strip()
            if part.isdigit():
                numbers.add(int(part))
    return numbers


# ------------------------------------------------------------- retrieval --
def search_body(vector: list[float], k: int, *, query: str = "", lexical: bool = True) -> dict:
    """The OpenSearch query for ``vector``, optionally boosted by the words.

    Pure k-NN is the honest demonstration of what the pipeline built, and it is
    also where a student sees the one failure mode that matters: a question
    phrased in words the document never uses still retrieves *something*,
    because k nearest neighbours are always nearest to something.

    ``lexical`` adds the query's own words back as a ``multi_match`` in a
    ``should``, which is a crude hybrid — OpenSearch scores the two clauses and
    adds them, with no normalization between a cosine similarity and a BM25
    score. Crude, but it is the cheap fix for the case k-NN alone is bad at:
    an exact term (a part number, a name) that embeds into nothing in
    particular. A real system would normalize the two with a search pipeline.
    """
    knn = {"knn": {"embedding": {"vector": list(vector), "k": k}}}
    if lexical and query.strip():
        clauses = [
            knn,
            {"multi_match": {
                "query": query,
                "fields": ["text^2", "headings", "heading_path"],
            }},
        ]
        body_query: dict = {"bool": {"should": clauses, "minimum_should_match": 1}}
    else:
        body_query = knn

    return {"size": k, "query": body_query, "_source": {"excludes": _EXCLUDED_SOURCE}}


def to_passages(hits: list[dict]) -> list[Passage]:
    """Search hits as numbered passages, in the order they were returned."""
    passages = []
    for n, hit in enumerate(hits, start=1):
        src = hit.get("_source", hit)
        origin = (src.get("metadata") or {}).get("origin") or {}
        passages.append(
            Passage(
                n=n,
                chunk_id=hit.get("_id", "") or src.get("chunk_id", ""),
                score=float(hit.get("_score") or 0.0),
                text=src.get("text") or "",
                filename=origin.get("filename", ""),
                doc_id=src.get("doc_id", "") or "",
                chunk_index=src.get("chunk_index"),
                headings=list(src.get("headings") or []),
                page_numbers=list(src.get("page_numbers") or []),
            )
        )
    return passages


def retrieve(client, index: str, vector: list[float], *, k: int = 6,
             query: str = "", lexical: bool = True) -> list[Passage]:
    """k nearest chunks to ``vector`` in ``index``."""
    if client is None:
        return []
    hits = client.search(index=index, body=search_body(vector, k, query=query, lexical=lexical))
    return to_passages(hits["hits"]["hits"])


# ---------------------------------------------------------------- prompt --
def build_prompt(question: str, passages: list[Passage], *, max_chars: int = 12_000) -> list[dict]:
    """The two messages sent to the chat model.

    The passages are numbered in the prompt with the same numbers the UI shows
    beside them, which is what makes a citation checkable: ``[3]`` in the answer
    and the third card on screen are the same chunk, and a student can read it
    and disagree.

    ``max_chars`` is a guard rather than a budget. Chunks are sized to
    ``EMBEDDING_MAX_TOKENS`` so ten of them fit any of these models comfortably;
    the cap only stops a pathological k from turning one question into a very
    expensive request.
    """
    blocks, used = [], 0
    for p in passages:
        block = f"[{p.n}] ({p.where})\n{p.text}".strip()
        if used + len(block) > max_chars:
            break
        blocks.append(block)
        used += len(block)

    context = "\n\n".join(blocks) if blocks else "(no passages were retrieved)"
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"Passages:\n\n{context}\n\nQuestion: {question}"},
    ]


# ------------------------------------------------------------ the whole --
def answer(client, index: str, question: str, *, k: int = 6, lexical: bool = True,
           dimension: int | None = None,
           watsonx: WatsonxConfig | None = None,
           embedding: EmbeddingConfig | None = None,
           generation: GenerationConfig | None = None) -> Answer:
    """Embed, retrieve, prompt, answer — the whole of it, in one call.

    Every failure comes back as :attr:`Answer.error` rather than an exception:
    this runs in a Streamlit rerun, where the useful outcome of "watsonx is not
    configured" is a sentence on the page and not a stack trace.
    """
    question = question.strip()
    if not question:
        return Answer(error="Ask a question first.")
    if client is None:
        return Answer(error="No OpenSearch connection — the Ask tab searches the index the pipeline wrote.")

    watsonx = watsonx or WatsonxConfig()
    embedding = embedding or EmbeddingConfig()
    generation = generation or GenerationConfig()

    try:
        watsonx.require()
    except RuntimeError as exc:
        return Answer(error=f"watsonx.ai is not configured: {exc}")

    # --- the query vector, from the model that embedded the chunks ---------
    try:
        with WatsonxEmbeddings(watsonx, embedding) as embedder:
            vector = embedder.embed([question])[0]
    except Exception as exc:  # noqa: BLE001 — every one of these is a message, not a crash
        return Answer(error=f"Could not embed the question: {exc}")

    # The mismatch that produces plausible nonsense rather than an error: the
    # search succeeds, the neighbours are simply not neighbours of anything the
    # question means. Cheap to catch here, invisible afterwards.
    if dimension and len(vector) != dimension:
        return Answer(error=(
            f"`{embedding.model_id}` returns {len(vector)}-dimensional vectors but `{index}` "
            f"maps {dimension}. The index was built for a different embedding model — "
            "rebuild it with `python scripts/setup_opensearch.py`, or set EMBEDDING_MODEL_ID "
            "back to the model it was built for."
        ))

    try:
        passages = retrieve(client, index, vector, k=k, query=question, lexical=lexical)
    except Exception as exc:  # noqa: BLE001
        return Answer(error=f"Search failed: {exc}")

    if not passages:
        return Answer(error=(
            f"Nothing in `{index}` to answer from. Upload a document to your bucket and wait "
            "for the pipeline to index it — the OpenSearch tab shows the document count."
        ))

    try:
        with WatsonxChat(watsonx, generation) as chat:
            text = chat.chat(build_prompt(question, passages))
    except Exception as exc:  # noqa: BLE001
        return Answer(passages=passages, model_id=generation.model_id,
                      error=f"The answer model failed: {exc}")

    return Answer(text=text.strip(), passages=passages, model_id=generation.model_id)
