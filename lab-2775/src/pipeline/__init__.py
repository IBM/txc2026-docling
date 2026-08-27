"""The streaming half of the system: Kafka chunk topic -> enrich -> embed -> sink.

Two entry points, and the package is laid out by what a module *is*:

    enrich_job.py  full_job.py   the two jobs — a list of stages each
    graph.py                     the wiring they are both assembled from
    config.py  kafka_io.py  watsonx.py
                                 plumbing: the environment, the connectors,
                                 the embeddings service
    stages/                      one module per Flink operator
    logic/                       pure, Flink-free, unit-tested — the thinking
                                 each stage does, importable from a script

Conversion and chunking are not here at all: Docling does both and produces the
chunk topic itself. The laptop-side producers live in ``src/producer``.
"""

__version__ = "0.1.0"
