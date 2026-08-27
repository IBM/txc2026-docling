"""Producer-side helpers: making chunk records *outside* Flink.

Nothing in :mod:`pipeline` imports this package, and nothing here runs in the
image. It exists for the one case Docling cannot serve itself — a local file,
which its ``kafka_chunks`` target cannot fetch (see
``scripts/ingest_folder.py``):

    chunking         HybridChunker -> the same records the target would write
    docling_client   upload a local file to Docling and get the document back

The records these produce must be indistinguishable from the service's, down to
the chunk id, so both build on :mod:`pipeline.logic.chunk_record` rather than
inventing a shape of their own.
"""
