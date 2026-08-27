"""Chunking on this machine, producing the records Docling would have produced.

Docling normally writes the chunk topic itself. It cannot do that for a local
file — the ``kafka_chunks`` target exists only on the source endpoints, and
``docling-core`` refuses any source URL that is not globally routable, so
serving the file from here is rejected too. ``scripts/ingest_folder.py`` closes
that gap, and this module is the part that has to be exact: the records it emits
go on the same topic as the converter's and must be indistinguishable from them,
down to the chunk ID (see :func:`pipeline.logic.chunk_record.stable_chunk_id`).

Laptop-side only: nothing in either Flink job imports this, which is why
`docling-core` and `transformers` are in pyproject's `local` dependency group
and not in the `image` one.
"""

from __future__ import annotations

from typing import Any, Iterator

from pipeline.logic import chunk_record


def build_chunker(tokenizer_id: str, max_tokens: int):
    """HybridChunker tokenized against the embedding model's tokenizer, so chunk
    sizes line up with that model's context window — the same setup
    ``scripts/saas_ingest.py`` asks the service for.

    ``tokenizer_id`` is a HuggingFace repo id. It is deliberately not
    ``EmbeddingConfig.model_id``, which since the move to watsonx.ai is a
    watsonx model id and would not resolve here."""
    from docling_core.transforms.chunker.hybrid_chunker import HybridChunker
    from docling_core.transforms.chunker.tokenizer.huggingface import HuggingFaceTokenizer
    from transformers import AutoTokenizer

    tokenizer = HuggingFaceTokenizer(
        tokenizer=AutoTokenizer.from_pretrained(tokenizer_id),
        max_tokens=max_tokens,
    )
    return HybridChunker(tokenizer=tokenizer, merge_peers=True)


def page_numbers(chunk) -> list[int]:
    pages: set[int] = set()
    for item in getattr(chunk.meta, "doc_items", []) or []:
        for prov in getattr(item, "prov", []) or []:
            if getattr(prov, "page_no", None) is not None:
                pages.add(prov.page_no)
    return sorted(pages)


def document_origin(doc) -> dict[str, Any]:
    """``metadata.origin`` from a ``DoclingDocument``.

    The converter fills this from the same ``DocumentOrigin``, so the keys and
    their values line up field for field.
    """
    origin = getattr(doc, "origin", None)
    if origin is None:
        return {}
    return {
        "mimetype": getattr(origin, "mimetype", None),
        "binary_hash": getattr(origin, "binary_hash", None),
        "filename": getattr(origin, "filename", None),
        "uri": str(origin.uri) if getattr(origin, "uri", None) else None,
    }


def _count_tokens(chunker, text: str) -> int | None:
    tokenizer = getattr(chunker, "tokenizer", None)
    count = getattr(tokenizer, "count_tokens", None)
    if count is None:
        return None
    try:
        return int(count(text))
    except Exception:  # noqa: BLE001 — token count is informational only
        return None


def chunk_records(
    chunker,
    doc,
    doc_id: str,
    *,
    document_hash: str = "",
    with_token_count: bool = False,
) -> Iterator[dict[str, Any]]:
    """Yield one wire-format record per chunk of ``doc`` (a ``DoclingDocument``).

    ``document_hash`` is :func:`pipeline.logic.chunk_record.document_hash` of the
    source bytes; pass it so the chunk IDs match what the service would assign
    for the same file.

    ``with_token_count`` is off by default because the converter does not send
    one — leaving it off keeps locally produced records byte-comparable with the
    service's, and the enrichment stage estimates the count either way.
    """
    origin = document_origin(doc)
    for idx, chunk in enumerate(chunker.chunk(dl_doc=doc)):
        # The contextualized form (headings prepended) is what the target puts
        # in `text`, and what gets embedded.
        text = chunker.contextualize(chunk=chunk)
        extra = {}
        if with_token_count:
            extra["token_count"] = _count_tokens(chunker, text)
        yield chunk_record.build(
            doc_id=doc_id,
            chunk_index=idx,
            text=text,
            document_hash=document_hash,
            origin=origin,
            headings=list(getattr(chunk.meta, "headings", []) or []),
            page_numbers=page_numbers(chunk),
            has_image=False,
            extra=extra or None,
        )
