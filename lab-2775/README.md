# LAB-2775 — From Bucket to RAG

**Event-driven Docling for watsonx and Confluent.** Drop a PDF in a bucket; a
few seconds later you can ask questions about it.

```
document → COS bucket ──event──▶ trigger ──▶ Docling (convert + chunk)
                                                 │
                                                 ▼  one Kafka message per chunk
    ws.NN.chunks ──▶ prepare ──▶ [guard → dedup] ──▶ embed (watsonx.ai)
                                                 │
                                                 ├──▶ ws.NN.enriched  (a topic)
                                                 └──▶ OpenSearch index → Ask
```

Everything after the upload is an event. Conversion and chunking happen in
**Docling**, not in Flink — its `kafka_chunks` target writes one Kafka message
per chunk, and the Flink job consumes that topic. The finished record goes to
both a Kafka topic *and* a k-NN index, and the dashboard's **Ask** tab embeds
your question with the same model, retrieves by vector, and has watsonx.ai
answer out of what it retrieved.

This directory is the whole lab: the two Flink pipelines, the Code Engine
trigger, the inspection dashboard, and the two scripts you drive them with.

## Start here

You are given a RHEL 9 VM. On it, run:

```bash
curl -L ibm.biz/txc26-2775-bootstrap | bash -
```

That installs `uv`, fetches this directory, builds the environments, and leaves
you with a `lab.yaml` to fill in. Then:

```bash
cd ~/txc2026-docling/lab-2775
# edit lab.yaml — your student id, your Docling URL and key, your bucket CRN
./setup.sh check          # is everything reachable?
./setup.sh topics         # your five Kafka topics
./setup.sh index          # your OpenSearch index
./pipeline.sh deploy simple
./pipeline.sh inspect     # the dashboard, on :8501
```

The lab guide handed out with the workshop is the long version, with the two
console steps (creating your bucket, subscribing it) that no script here does.

## The two scripts

Everything you own is namespaced by `student.id` in `lab.yaml`, so thirty
people run these at the same time against one cluster.

| `./setup.sh` | |
|---|---|
| `check` | validate `lab.yaml`, then probe Kafka, CMF, OpenSearch and watsonx |
| `topics` | create your five topics — the brokers do not auto-create |
| `index` | create your OpenSearch k-NN index |
| `info` | every name the next step needs, in the spelling it wants |

| `./pipeline.sh` | |
|---|---|
| `deploy simple` | chunks → prepare → embed → sink |
| `deploy full` | …plus a broadcast policy, a PII guard, dedup, and two audit topics |
| `status` | which pipeline is deployed, and what it is doing |
| `policy '<json>'` | change the running job without redeploying it |
| `inspect` | the dashboard, pointed at your topics |
| `delete` | remove it |

`deploy` is a **switch, not an add**: exactly one pipeline runs at a time, so it
deletes what is there first. A CMF `FlinkApplication` is application mode — one
JobManager, one job — and the cluster is packed close to its memory ceiling.

## What is in here

| | |
|---|---|
| `src/pipeline/` | the Flink job: two entry points, the wiring they share, one module per operator, and the pure decisions under `logic/` |
| `src/producer/` | chunking and the Docling upload client — used by `scripts/`, never by the job |
| `src/docling_trigger/` | the Code Engine app: a COS object-write event becomes a Docling submission |
| `src/inspector/` | the dashboard, including its Ask tab (`rag.py`, four function calls long — the hard part was everything upstream) |
| `src/labtools/` | `lab.yaml` → the environment, one Kafka client config, one record renderer |
| `images/` | the two Dockerfiles, one per deployed component |
| `flink/` | the `FlinkApplication` descriptors CMF is given |
| `scripts/` | the executables the two front-ends call |
| `tests/` | everything that does not need a cluster (`uv run pytest`) |
| `docs/ARCHITECTURE.md` | why it is shaped like this — read this before changing anything |

## Running it without a workshop

The two images are published and the job is submitted through CMF, so a full
run needs a Confluent cluster with CMF, a Docling service, a watsonx.ai project
and an OpenSearch cluster. The parts that need none of that:

```bash
uv run pytest                                   # the pure logic, the record
                                                # shape, the trigger, the RAG prompt
uv run scripts/sample_chunks.py                 # a crafted document with PII and
                                                # an exact duplicate in it
uv run scripts/drain_topic.py --topic <topic>   # read any topic back
```

## Licence

See the repository root.
