"""The retrieval-augmented answer, in the parts that are decidable without a cluster.

``src/inspector/rag.py`` is deliberately free of streamlit and pyflink, so
the query it builds, the passages it numbers and the prompt it sends can all be
checked here. What is left for a live system is the two HTTP calls.

The cases below are the ones that produce a *plausible* wrong answer rather than
an error, which is the failure mode a RAG demo has to be built against: a
citation that points at the wrong passage, a prompt that lets the model answer
from its own knowledge, a query vector from the wrong model.
"""

from __future__ import annotations

import pytest

from inspector import rag


def hit(n: int, text: str = "some text", *, filename: str = "manual.pdf",
        headings: list[str] | None = None, pages: list[int] | None = None) -> dict:
    return {
        "_id": f"chunk-{n}",
        "_score": 1.0 / n,
        "_source": {
            "text": text,
            "doc_id": "doc-a",
            "chunk_index": n,
            "headings": headings if headings is not None else ["Operation"],
            "page_numbers": pages if pages is not None else [n],
            "metadata": {"origin": {"filename": filename}},
        },
    }


# --- citations -------------------------------------------------------------

@pytest.mark.parametrize(
    "text, expected",
    [
        ("as stated [1]", {1}),
        ("both [1][3]", {1, 3}),
        ("a list [1, 2, 3]", {1, 2, 3}),
        ("spaced [ 2 , 4 ]", {2, 4}),
        ("no citations at all", set()),
        ("not a citation [see below]", set()),
    ],
)
def test_cited_numbers(text, expected):
    assert rag.cited_numbers(text) == expected


def test_answer_reports_which_passages_it_used():
    answer = rag.Answer(text="The pressure is 3.2 bar [2].",
                        passages=rag.to_passages([hit(1), hit(2), hit(3)]))
    assert answer.cited == {2}
    assert answer.ok


def test_an_answer_with_an_error_is_not_ok():
    assert not rag.Answer(text="something", error="watsonx refused").ok


# --- the search body -------------------------------------------------------

def test_pure_knn_when_the_keyword_boost_is_off():
    body = rag.search_body([0.1, 0.2], 5, query="anything", lexical=False)
    assert body["query"] == {"knn": {"embedding": {"vector": [0.1, 0.2], "k": 5}}}
    assert body["size"] == 5


def test_the_vector_is_never_returned_in_the_source():
    """A 768-float vector per hit is most of the response body, and nothing
    downstream reads it."""
    body = rag.search_body([0.1], 3)
    assert body["_source"]["excludes"] == ["embedding"]


def test_the_keyword_boost_keeps_the_knn_clause():
    body = rag.search_body([0.1], 4, query="coolant pressure", lexical=True)
    clauses = body["query"]["bool"]["should"]
    assert any("knn" in c for c in clauses)
    assert any("multi_match" in c for c in clauses)


def test_an_empty_question_does_not_produce_a_lexical_clause():
    """`should` with only a k-NN clause is the same query; an empty multi_match
    is not, and OpenSearch scores it as a match on everything."""
    body = rag.search_body([0.1], 4, query="   ", lexical=True)
    assert body["query"] == {"knn": {"embedding": {"vector": [0.1], "k": 4}}}


# --- passages --------------------------------------------------------------

def test_passages_are_numbered_from_one_in_search_order():
    passages = rag.to_passages([hit(3), hit(1), hit(2)])
    assert [p.n for p in passages] == [1, 2, 3]
    assert [p.chunk_index for p in passages] == [3, 1, 2]


def test_provenance_survives_a_chunk_with_no_structure():
    """headings and page_numbers are opt-in on Docling's target, so a chunk may
    legitimately have neither."""
    passage = rag.to_passages([hit(1, headings=[], pages=[])])[0]
    assert passage.where == "manual.pdf"


def test_provenance_reads_a_page_range():
    passage = rag.to_passages([hit(1, headings=["A", "B"], pages=[4, 5, 6])])[0]
    assert "pp. 4–6" in passage.where
    assert "A › B" in passage.where


# --- the prompt ------------------------------------------------------------

def test_the_prompt_numbers_passages_as_the_ui_shows_them():
    passages = rag.to_passages([hit(1, "first"), hit(2, "second")])
    context = rag.build_prompt("why?", passages)[1]["content"]
    assert "[1]" in context and "first" in context
    assert "[2]" in context and "second" in context
    assert context.rstrip().endswith("Question: why?")


def test_the_system_prompt_forbids_answering_from_memory():
    """The whole claim of the demo is that the answer came out of these
    documents, and the way to show it is to watch the model refuse."""
    system = rag.build_prompt("q", [])[0]["content"].lower()
    assert "only" in system
    assert "do not guess" in system


def test_an_oversized_context_is_truncated_rather_than_sent():
    """A pathological k must not turn one question into a very expensive
    request; the cap drops whole passages rather than cutting one in half."""
    passages = rag.to_passages([hit(n, "x" * 500) for n in range(1, 11)])
    context = rag.build_prompt("q", passages, max_chars=1200)[1]["content"]
    assert "[1]" in context and "[2]" in context
    assert "[3]" not in context
    # ...and what survives is whole passages, not a passage cut off mid-word.
    assert context.count("x" * 500) == 2


def test_no_passages_still_produces_a_well_formed_prompt():
    messages = rag.build_prompt("q", [])
    assert [m["role"] for m in messages] == ["system", "user"]
    assert "no passages" in messages[1]["content"]


# --- answer(), in the paths that never reach the network -------------------

def test_a_blank_question_is_refused_before_anything_is_called():
    assert rag.answer(object(), "idx", "   ").error == "Ask a question first."


def test_no_client_is_a_message_not_a_crash():
    assert "OpenSearch" in rag.answer(None, "idx", "why?").error
