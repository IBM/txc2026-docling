"""One module per Flink operator, in the order a record meets them.

    prepare  drop empties, normalize, enrich       (both jobs)
    guard    apply the broadcast policy: redact, gate, fork  (full_job only)
    dedup    keyed TTL'd state on the fingerprint  (full_job only)
    embed    watsonx.ai, in timer-bounded batches   (both jobs)
    sink     the terminal operators SINK_TYPE names — a list, not a
             choice: the workshop writes the output topic and the index

They are wired into a job by :mod:`pipeline.graph`, never by importing each
other. Each holds only the Flink mechanics — state, timers, side outputs — and
delegates the actual decision to :mod:`pipeline.logic`, which is why a stage can
be several stages fused into one operator without costing any clarity.
"""
