# How this is put together

Read this before changing anything. Most of what follows is a decision that
looked arbitrary until it cost somebody an afternoon.

## The shape

    documents → COS bucket ──event──▶ trigger ──▶ Docling (convert + chunk)
                                                      │ one Kafka message per chunk
              ws.NN.chunks ──▶ prepare ──▶ [guard → dedup] ──▶ embed (watsonx.ai)
                                                      ├──▶ ws.NN.enriched
                                                      └──▶ OpenSearch → the Ask tab

Conversion and chunking happen in **Docling**, not in Flink: its `kafka_chunks`
target writes one Kafka message per chunk, and the job consumes that topic.

Two pipelines, and they are the lab's two steps — `./pipeline.sh deploy
simple|full` switches between them:

- `pipeline/enrich_job.py` — **simple**: the spine above, nothing else, and
  deliberately stateless: no keyed state, one shuffle in the whole graph.
- `pipeline/full_job.py` — **full**: the same spine plus a control plane — a
  broadcast policy topic and the PII guard it drives, with quarantine and
  rejection side outputs — and the dedup stage, which is where a student meets
  keyed TTL'd state for the first time.

Both are assembled from the same helpers in `pipeline/graph.py`, so the only
difference visible in the job modules is those two stages.

The sink is a **fan-out, not a choice**: `SINK_TYPE` names a list, and the
default is `kafka,opensearch` — the finished record goes to the output topic
*and* into the student's index. Fanning out costs no shuffle: each terminal
hangs off the same already-partitioned stream. Every job must keep working with
each name alone, because OpenSearch is not always reachable and a pipeline that
cannot run without its index cannot be debugged.

## The five packages

    src/pipeline/          the Flink job — the two entry points, graph.py, stages/, logic/
    src/producer/          chunking and the Docling upload client; scripts/ only
    src/docling_trigger/   the Code Engine app
    src/inspector/         the dashboard, app.py included
    src/labtools/          lab.yaml → the environment; the Kafka client; the record renderer

One directory per component, all the same shape, all installed editable by
`uv sync` — so `import pipeline` works in the tests, the scripts and the
dashboard with no `PYTHONPATH` and no `sys.path` patching anywhere.
`PYTHONPATH` survives only *inside* the two images, which install with
`--no-install-project` and copy in one package each.

`scripts/` holds executables; `src/` holds what is imported. That is the whole
rule, and it is why `labtools` exists at all: the dashboard needs the Kafka
client configuration and the record renderer, and while those lived beside the
scripts something had to keep patching the path to reach them.

## Rules that are load-bearing

- **A stage holds Flink mechanics, `logic/` holds the decision.** Each
  `pipeline/stages/*.py` is one operator and delegates what to *do* to a pure
  function. Heavy resources (HTTP clients, the OpenSearch client) are created in
  `open()`, never in `__init__`, which must stay cheap to serialize into the job
  graph.
- **One operator can be several stages, and in `full_job` it has to be.** Every
  Python operator runs its own Python worker process unless PyFlink's chaining
  optimizer fuses it with its neighbour — and that optimizer cannot be used in a
  job with a broadcast operator. So `stages/prepare.py` (drop + normalize +
  enrich) and `stages/dedup.py` (tag + drop) are hand-fused. The stages still
  exist as functions under `logic/`, which is what makes fusing them cost no
  clarity.
- **Embedding is a network call, not a model.** `pipeline/watsonx.py` calls
  watsonx.ai; nothing loads torch. That is what took the image from 4.05 GB to
  ~1.6 GB and a Python worker from 883 MiB to ~200 MiB, which is what lets one
  pipeline per student fit on a shared cluster. Two consequences: the embed
  stage batches (one request per chunk would be 1000 round trips per run), and
  it can fail, so it retries 429/5xx and refreshes the IAM token on 401.
- **The chunk record is Docling's, and it is the only one.** `doc_id`,
  `metadata.origin`, `chunk_index`, `chunk_id`, and — when the producer asked
  for them — `headings` / `page_numbers`. Stages *add* derived fields and never
  rename or move what is there. There is no adapter and no internal shape.
  `pipeline/logic/chunk_record.py` defines the record, its accessors and its ID
  scheme.
- **`chunk_id` belongs to the producer.** It is
  `sha256(f"{sha256(file_bytes)}:{doc_id}:{chunk_index}")`, matching
  `docling_jobkit`'s target, and it is the index's document id and the thing
  duplicate deliveries collapse on. Never invent a different one.
- **Records flow as JSON strings** between operators (`SimpleStringSchema`),
  accumulating fields stage by stage.
- **Two model ids, and they are not the same string.** `EMBEDDING_MODEL_ID` is a
  *watsonx.ai* id and names what produces the vectors. `CHUNK_TOKENIZER_ID` is a
  *HuggingFace* repo id and only ever supplies a tokenizer to the producers, so
  chunks are sized to the same budget. Feeding one to the other fails.
- **The model id is regional.** Each watsonx datacentre publishes its own
  catalogue; ca-tor serves exactly four embedding models —
  `ibm/granite-embedding-278m-multilingual` and `ibm/slate-125m-english-rtrvr-v2`
  at 768 dimensions, `ibm/slate-30m-english-rtrvr-v2` at 384,
  `intfloat/multilingual-e5-large` at 1024. A model the region does not serve is
  a 404 on the first batch, not a fallback. `./setup.sh check` asks the
  catalogue rather than assuming.
- **The embedding dimension must match the k-NN mapping.**
  `WatsonxEmbeddings` checks the first response and kills the job on a mismatch,
  because otherwise it surfaces much later as a rejected bulk write. The Ask tab
  checks it from the other end: a query embedded by a *different* model does not
  fail, it returns k nearest neighbours that are simply unrelated.
- **One index per student, and its name is not a preference.** The cluster's
  security plugin grants `studentNN` full rights on `studentNN-*` and nothing
  anywhere else, so `OPENSEARCH_INDEX` is derived from the account. A name
  outside the prefix is a 403 on every write from a pipeline that otherwise
  looks perfectly healthy. Anything phrased over *all* indices — `_cat/indices`,
  `get_alias("*")` — is a 403 too, so the dashboard asks over the student's own
  prefix and treats a refusal as "not yours to see".

## Configuration

`lab.yaml` is what a student edits. `labtools/config.py` turns it into the
environment variables every component already reads — `pipeline.config`, the
`FlinkApplication` descriptor, the TaskManager's own environment, the dashboard.
Three layers, later winning: the defaults in that module, `lab.yaml`, then the
process environment.

Everything per-student is **derived** from `student.id` and wins over the
environment: the topics, the application name, the consumer group, the
OpenSearch account and its index. That is not tidiness — the three collisions
that matter are silent rather than loud. A shared consumer group makes Kafka
*split the partitions between two students*, so most of the class sees no data
at all. A shared application name means one deploy replaces another's. A shared
output topic interleaves results nobody can then find.

The descriptor is rendered by `labtools.config.render`, not by `envsubst`, and
the difference is the point: `envsubst` substitutes an **empty string** for a
name it does not know, so a typo produced a `FlinkApplication` that CMF
accepted, that started, and that then read from a topic called `""` forever.
It also means the lab VM needs no gettext.

## Things that will bite you

- **The PII guard is a demonstrator.** It is there to make the side-output and
  broadcast-policy mechanics concrete with something recognisable flowing
  through them, and `logic/pii.py` is regular expressions and two checksums —
  it finds structured identifiers (e-mail, card, IBAN) and nothing else. Names,
  addresses and dates of birth pass through untouched, Luhn still admits about
  one arbitrary digit run in ten, and `phone` and `ip_address` are off by
  default because a numeric table row and a version string match them. A
  corpus that has actually been de-identified needs an entity model behind the
  async stage. Do not let the topic named `ws.NN.pii` imply more than that.
- **Docling cannot fetch a local file.** The `kafka_chunks` target exists only
  on the *source* endpoints, and `docling-core`'s `_is_safe_url` rejects any URL
  that is not globally routable — so serving the file from your own machine does
  not work either. Local files go through `scripts/ingest_folder.py`.
- **The Docling client drops the chunker discriminator.** It serializes options
  with `exclude_defaults=True`, which removes `chunking_options.chunker`, and the
  server answers 422 "Unable to extract tag using discriminator". The override in
  `scripts/saas_ingest.py::_client` puts it back. `docling_trigger` sidesteps it
  by building the request as a plain dict.
- **Flink's Kafka client is the Java one**, with no "skip TLS verification"
  switch. Against a SASL_SSL listener signed by a private CA it needs the CA as a
  PEM truststore, and the file must exist inside the container. *Docling* needs
  the same CA but cannot be given a path — it runs in someone else's cloud — so
  the `kafka_chunks` target carries it base64-encoded as `auth.ca_cert`.
- **The SASL login module class is the shaded one.**
  `flink-sql-connector-kafka` is a fat jar that relocates kafka-clients, so
  `org.apache.kafka.common.security.plain.PlainLoginModule` is not on the
  classpath and the job dies at start with "No LoginModule found". The JAAS
  string must name
  `org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule`.
- **`python.operator-chaining.enabled=false`** (in `full_job`) is a workaround,
  not tuning: PyFlink's chaining optimizer cannot rewrite a broadcast-connected
  operator and fails at submit time with `NoSuchFieldException: regularInput`.
  The flag is global — there is no per-operator escape — and what it costs is
  Python worker processes, one per Python operator. The per-variant TaskManager
  sizing follows from the same decision, which is why both live together in
  `labtools/config.py` and neither is overridable from the environment.
- **That flag does not change the boxes in the Flink UI.** Those are job
  vertices, and they come from Flink's own operator chaining: a chain breaks at
  a shuffle (every `key_by`), at a two-input operator, and where a job asks with
  `start_new_chain()`. `enrich_job` draws three vertices, `full_job` five. Boxes
  are free; operators are not.

## The two images

`images/pipeline.Dockerfile` and `images/trigger.Dockerfile` build from the
project root and follow the same recipe: `uv sync --frozen --no-install-project
--no-default-groups --group <image|trigger>`, then one narrow `COPY` of the one
package they carry, on `PYTHONPATH`.

What keeps them from mixing is that `COPY` and that group, not the shape of the
build context — so both are asserted in `tests/test_images.py`. A trigger image
that quietly grew apache-flink looks like a slow build, not like a bug.

The pipeline image is built on `redhat/ubi9`, not on `cp-flink`: PyFlink's
`pemja` dependency ships no linux/arm64 wheel, so on Apple Silicon it compiles
from source and needs gcc and JDK headers, which the cp-flink runtime base
(RHEL 9, no package manager, JRE only) does not have. The builder stage must
stay on a RHEL 9 userspace so the compiled venv matches the runtime's glibc.
`pyflink/lib` and `pyflink/opt` are deleted from the venv: the pip wheel ships a
whole Flink distribution, and `FLINK_HOME=/opt/flink` is set by the base image,
so they are ~307 MB of shadow jars.

The `image` group must stay compatible with the base image's Flink version
(`cp-flink:2.1.3-cp2` → Flink 2.1). Three things move together on an upgrade:
`apache-flink` in that group, the connector URL in `scripts/fetch_jars.sh`, and
`flinkVersion` + `jarURI` in `flink/application*`. The ceiling is CMF's, not the
image's: this CMF accepts `flinkVersion` only up to `v2_1`, so 2.2 — and with it
PyFlink's async I/O — needs CMF ≥ 2.4.1 first.

## Testing

`uv run pytest` runs everything that does not need a cluster. Anything importing
`pyflink` is skipped outside the image. The pure halves are tested directly:
`logic/`, `labtools/`, the trigger's event parsing and presigning, and
`inspector/rag.py` — which imports neither streamlit nor pyflink precisely so
that the search body, the numbered prompt and the citation parser can be checked
here.
