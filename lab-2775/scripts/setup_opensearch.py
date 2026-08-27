#!/usr/bin/env python3
"""Create the OpenSearch index with a knn_vector mapping for the chunk embeddings.

Run once before starting the pipeline::

    python scripts/setup_opensearch.py     # reads OPENSEARCH_* from the env
    ./setup.sh index                       # ...with your student values filled in

In the workshop every student owns one index and only their own: the cluster
grants ``studentNN`` full rights on ``studentNN-*`` and nothing else, so the
name is derived from the account rather than chosen. ``OpenSearchConfig.require``
is what refuses a name outside that prefix here, where the message can say so,
rather than at the pipeline's first write, where it is a 403 in a TaskManager
log.

Nothing here is destructive: an index that already exists is left alone and its
mapped dimension is reported back, because that is the mismatch worth catching
— an index built for a 384-dimensional model rejects every 768-dimensional
write, one record at a time, with the job otherwise healthy.
"""

from __future__ import annotations

import os
import sys

# Allow running from the repo root without installing the package.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from opensearchpy import OpenSearch  # noqa: E402

from pipeline.config import EmbeddingConfig, OpenSearchConfig  # noqa: E402


def index_body(dimension: int) -> dict:
    return {
        "settings": {"index": {"knn": True}},
        "mappings": {
            "properties": {
                # --- the chunk record as Docling's kafka_chunks target writes it
                "doc_id": {"type": "keyword"},
                "chunk_index": {"type": "integer"},
                "chunk_id": {"type": "keyword"},
                "text": {"type": "text"},
                "headings": {"type": "text"},
                "page_numbers": {"type": "integer"},
                "metadata": {
                    "properties": {
                        "has_image": {"type": "boolean"},
                        "origin": {
                            "properties": {
                                "filename": {"type": "keyword"},
                                "mimetype": {"type": "keyword"},
                                "uri": {"type": "keyword"},
                                # Does not fit a signed 64-bit long, so it is
                                # indexed as a keyword rather than a number.
                                "binary_hash": {"type": "keyword"},
                            }
                        },
                    }
                },
                # --- fields added by pipeline.enrich_job -------------------
                "fingerprint": {"type": "keyword"},
                "char_count": {"type": "integer"},
                "word_count": {"type": "integer"},
                "token_count": {"type": "integer"},
                "avg_word_len": {"type": "float"},
                "heading_path": {"type": "keyword"},
                "section_depth": {"type": "integer"},
                "page_start": {"type": "integer"},
                "page_end": {"type": "integer"},
                "has_table": {"type": "boolean"},
                "has_list": {"type": "boolean"},
                "has_code": {"type": "boolean"},
                "has_formula": {"type": "boolean"},
                "script": {"type": "keyword"},
                "alpha_ratio": {"type": "float"},
                "digit_ratio": {"type": "float"},
                "symbol_ratio": {"type": "float"},
                "quality_flags": {"type": "keyword"},
                "keep": {"type": "boolean"},
                "ingested_at": {"type": "date"},
                "embedding": {
                    "type": "knn_vector",
                    "dimension": dimension,
                    "method": {
                        "name": "hnsw",
                        "space_type": "cosinesimil",
                        "engine": "lucene",
                    },
                },
            }
        },
    }


def mapped_dimension(client: OpenSearch, index: str) -> int | None:
    """The ``knn_vector`` dimension the index was actually created with.

    Worth asking for, because an index that already exists was created by an
    earlier run — possibly with a different ``EMBEDDING_MODEL_ID``, and the
    models this lab can use are 384-, 768- and 1024-dimensional. The pipeline
    finds out at the first bulk write, as a rejection per record.
    """
    try:
        mapping = client.indices.get_mapping(index=index)
        props = mapping[index]["mappings"]["properties"]
        return int(props["embedding"]["dimension"])
    except Exception:
        return None


def summary(
    os_cfg: OpenSearchConfig,
    emb_cfg: EmbeddingConfig,
    created: bool,
    mapped: int | None = None,
) -> str:
    """What the next step has to be given, in the spelling it wants it in.

    Same shape as ``lab_topic_summary`` in ``scripts/lib.sh``, and for
    the same reason: the index name has to match ``OPENSEARCH_INDEX`` in the
    job's environment and the dimension has to match what
    ``EMBEDDING_MODEL_ID`` actually returns, and neither mismatch is visible
    until records are already being rejected.
    """
    bold, off = ("\033[1m", "\033[0m") if sys.stdout.isatty() else ("", "")
    rows = [
        ("index", os_cfg.index, "created" if created else "already existed"),
        ("hosts", os_cfg.hosts, f"as {os_cfg.username}" if os_cfg.username else ""),
        ("dimension", str(emb_cfg.dimension), f"must match {emb_cfg.model_id}"),
    ]
    out = [f"\n{bold}OpenSearch index{off}"]
    out += [f"  {k:<12} {v:<24} {note}".rstrip() for k, v, note in rows]
    if mapped is not None and mapped != emb_cfg.dimension:
        out += [
            f"  {'!':<12} the existing index maps embedding as {mapped}-dimensional,",
            f"  {'':<12} not {emb_cfg.dimension}. Every write will be rejected. Delete the",
            f"  {'':<12} index and re-run, or set EMBEDDING_MODEL_ID back to the",
            f"  {'':<12} model it was built for.",
        ]
    out += [
        f"\n{bold}Carry into the pipeline's environment{off}",
        f"  OPENSEARCH_INDEX={os_cfg.index}",
        f"  OPENSEARCH_HOSTS={os_cfg.hosts}",
        f"  OPENSEARCH_USERNAME={os_cfg.username}",
        f"  EMBEDDING_DIMENSION={emb_cfg.dimension}",
        "  SINK_TYPE=kafka,opensearch",
        "",
        "  SINK_TYPE names a list, and the workshop's default names both: the",
        "  finished record goes to the output topic *and* into this index. The",
        "  topic is what `scripts/drain_topic.py` prints; the index is what the",
        "  inspector's Ask tab answers questions out of.",
    ]
    return "\n".join(out)


def main() -> None:
    os_cfg = OpenSearchConfig()
    emb_cfg = EmbeddingConfig()
    # Refuses an index outside this account's namespace, a missing password and
    # a CA path that is not there — the three that otherwise surface as a 403
    # or a handshake failure well after this script has said "created".
    os_cfg.require()
    client = OpenSearch(**os_cfg.client_kwargs())
    created = not client.indices.exists(index=os_cfg.index)
    if created:
        client.indices.create(index=os_cfg.index, body=index_body(emb_cfg.dimension))
        mapped = None
    else:
        mapped = mapped_dimension(client, os_cfg.index)
    print(summary(os_cfg, emb_cfg, created, mapped))


if __name__ == "__main__":
    main()
