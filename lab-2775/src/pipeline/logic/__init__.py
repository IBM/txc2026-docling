"""The pure half: no ``pyflink``, no network, no state.

    chunk_record   the input contract — accessors, the ID scheme, the builder
    enrichment     normalization and the derived fields
    pii            detectors (Luhn, IBAN mod-97) and redaction
    policy         the broadcast control record and its validation
    keys           the key selectors the two shuffles partition on

Everything here is deterministic and importable from a laptop, which is what
lets the operators and the producer scripts share it instead of drifting apart,
and what makes it all unit-testable without a Flink runtime (``tests/``).
"""
