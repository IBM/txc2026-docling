# Pipeline inspector

The window onto **LAB-2775 — From Bucket to RAG: Event-Driven Docling for
watsonx and Confluent**: a small Streamlit app that shows what the pipeline is
doing, from a laptop.

It answers the four questions you have while a job is running:

* **which pipeline is deployed** — the workshop has two, a student runs one at a
  time, and the dashboard asks the control plane rather than trusting a radio
  button;
* **how much is on each topic** — messages per topic, and the rate they are
  arriving at;
* **which stage is actually processing** — per-stage busy percentages when a
  JobManager is reachable, inferred from topic throughput when it is not;
* **what the graph looks like** — the stages of `enrich_job` or `full_job`, in
  the shape the code builds them, coloured by what each one is doing;
* **what the messages look like** — any topic, newest or oldest first, as a
  table and as raw JSON; and the OpenSearch index the job is writing.

...and then the one question the whole lab was building towards, in the **Ask**
tab: **does any of this answer anything?** A question typed there is embedded by
the same model that embedded the chunks, the nearest chunks are pulled out of
the index by k-NN, and a watsonx.ai chat model is asked to answer out of those
chunks and nothing else. Every passage it was given is shown beneath the
answer, cited ones expanded, so a claim and the text it came from can be read
side by side — including when they disagree.

It also answers the question that comes *before* those four — **how do I get a
document in?** — with the three steps that start the pipeline and the link to
the bucket you upload to.

It is not part of the pipeline image. Nothing in `Dockerfile` copies this
directory, `.dockerignore` keeps it out of the build context, and its
dependencies (`streamlit`, `confluent-kafka`, `requests`, `opensearch-py`) have
nothing to do with the job's.

What it does share is `src/`: `inspector/` imports `pipeline.config` and
`pipeline.watsonx` rather than restating them, which is what keeps the stage
graph, the sink names and the watsonx client from drifting away from the
pipeline they describe. `inspector/rag.py` imports neither streamlit nor
pyflink, so its search body, prompt and citation parser are unit-tested in
`tests/test_rag.py` without a cluster, a model or a browser.

## Run it

```bash
cd dashboard
./run.sh                       # creates ./.venv on first run, opens :8501
```

or, from the repo root, `make inspect`. Both end up at
<http://localhost:8501>.

## Where documents come in

There is one way a document enters the pipeline, and none of it is Flink:

```
upload → IBM COS bucket ──object-created event──▶ Docling SaaS (convert + chunk)
       → docling.chunks → Flink
```

Uploading an object to the bucket is the only manual step — nothing polls and
nothing is scheduled, so a document is either in the bucket or it is not in the
system. The write raises an event, the event triggers a Docling job, and
Docling's `kafka_chunks` target writes one Kafka message per chunk.

The dashboard draws those two as external nodes (blue, clickable) in front of
the chunk topic, states them as two numbered steps at the top of the Stages tab,
and puts the bucket behind an **Upload a document** button in the sidebar. It
cannot *probe* either of them — they run in someone else's cluster — so the
first evidence they ran is the message count on `docling.chunks` moving.

The scripts under `scripts/` (`make ingest`, `make ingest-files`) write the same
chunk topic by hand. That is a development shortcut around this path, not a
second way in, and it is deliberately not on the diagram.

### Configuring the links

The bucket's console URL is long enough that handing it to thirty students is
asking for thirty truncated URLs:

```
https://cloud.ibm.com/objectstorage/crn%3Av1%3Abluemix%3Apublic%3Acloud-object-storage%3Aglobal%3Aa%2F793a…%3Ae8db…%3A%3A?paneId=bucket_overview&bucket=bucket-etftth-input-docs&bucketRegion=ca-tor&endpoint=s3.ca-tor.cloud-object-storage.appdomain.cloud
```

Every part of it except the region is already in the bucket's CRN, which the
console offers as a copy button — so that, plus the region, is the
configuration, and `inspector/cos.py` composes the URL (tested against the real
one in `tests/test_cos_url.py`):

| variable | what it is |
|---|---|
| `COS_BUCKET_CRN` | `crn:v1:bluemix:public:cloud-object-storage:global:a/<account>:<instance>:bucket:<name>` — the string to hand out |
| `COS_BUCKET_REGION` | e.g. `ca-tor`; the only part the CRN does not carry |
| `COS_BUCKET_URL` | optional, a ready-made console link — wins over the composed one |
| `COS_ENDPOINT` | optional, if the bucket is not on the region's public S3 endpoint |
| `COS_BUCKET` | optional display name; defaults to the CRN's |
| `DOCLING_SERVICE_URL` | the Docling instance — the second stage links to its *workbench*, which `labtools.config.docling_workbench_url` derives from this: `https://api.<region>.dcls.saas.ibm.com/<instance>` is the same instance as `https://workbench.<region>.dcls.saas.ibm.com/instances/<instance>`, so there is nothing else to configure |

None of them is a secret. With no bucket configured both stages are drawn
greyed out and the dashboard says which variable is missing rather than
inventing a path — which is what the local profile looks like, since the
compose stack has no bucket and no events.

## Which pipeline is on screen

The workshop deploys one of two applications per student and switches between
them by deleting the one that is there (`./pipeline.sh deploy simple|full`).
So *which pipeline am I looking at* is a fact about the cluster, and getting it
wrong is the one mistake a teaching dashboard cannot afford: every stage on the
diagram, every topic panel and every count would be describing a job that is not
running.

`inspector/deployment.py` asks instead of assuming, and it asks the thing that
did the deploying:

* **CMF** — one `GET` of the application collection carries every descriptor,
  and `spec.job.args` holds the `-pym` module the job was actually submitted
  with. An application named `ws-07-full` running the simple module is reported
  as what it *runs*. The same response also reveals the case worth warning
  about — both applications deployed at once, which is two TaskManagers held for
  one student.
* **Flink REST** — the local stack has no applications, but the two jobs call
  `env.execute()` with different names, so a running job identifies itself.

`WS_ID=07` is the only per-student setting: the application names
(`ws-07-simple`, `ws-07-full`, and the pre-variant `ws-07` if an earlier fan-out
left one) are built from it, exactly as the scripts build them.
`./pipeline.sh inspect` — and the instructor's fan-out, once per student — set it along
with the topics.

The sidebar's **Pipeline** control defaults to *Deployed (detected)*; the two
manual choices draw a pipeline whether or not it is running, which is how you
read the next step before taking it. When nothing is deployed the dashboard says
so and gives the command, rather than showing CMF's 404 as an error.

## What it reads

Two targets, switched in the sidebar:

| | Kafka | Job status | Per-stage metrics | Documents arrive by |
|---|---|---|---|---|
| **Local stack** (`make up`) | `localhost:29092` | Flink REST on `localhost:8081` | yes | no bucket — `make ingest` writes the topic by hand |
| **Hosted** (your `lab.yaml`) | the SASL_SSL listener | CMF application status | no — see below | upload → COS bucket → event → Docling SaaS |

Configuration comes from your `lab.yaml` (topics, stage toggles, credentials,
CMF) with localhost addresses substituted for the local profile. Any variable
already set in your shell wins, so

```bash
KAFKA_CHUNKS_TOPIC=my.topic ./run.sh
```

works the way it does for every other script here. The Configuration tab shows
exactly what was resolved, with secrets masked.

**Everything is read-only.** Topic watermarks, committed offsets, `GET`s
against Flink and CMF, searches against OpenSearch. No consumer group is
joined and no offset is ever committed, so refreshing cannot move a running
job's offsets or trigger a rebalance — the same discipline
`scripts/drain_topic.py` follows. The Ask tab is the one panel that *calls* a
service rather than reading one — two watsonx.ai requests per question — and it
is still read-only with respect to the pipeline: it writes nothing to the index
and touches no offset.

### What the Ask tab needs

| | where it comes from |
|---|---|
| `OPENSEARCH_HOSTS` / `_INDEX` / `_USERNAME` | derived from `student.id` by `./pipeline.sh inspect` |
| `OPENSEARCH_PASSWORD` | the classroom handout, pasted into `lab.yaml` — the *deployed job* takes the same password from a Kubernetes Secret instead |
| `OPENSEARCH_CA_LOCATION` | `./opensearch/root-ca.pem`, checked in. The cluster's certificate is from a private CA, exactly like the Kafka brokers'; without this the tab reads "not reachable" when the truth is "I do not trust this certificate" |
| `WATSONX_APIKEY`, `WATSONX_PROJECT_ID` | `watsonx.api_key` and `watsonx.project_id` in `lab.yaml` — the same two the embed stage uses, except that the *deployed job* takes the key from a Kubernetes Secret instead |
| `WATSONX_LLM_MODEL_ID` | the model that answers. ca-tor serves two — `meta-llama/llama-3-3-70b-instruct` and `mistralai/mistral-small-3-1-24b-instruct-2503` — and a third is a 404, not a fallback |

An empty index is not a failure and the tab says which of the three it is: no
cluster, no index (`./setup.sh index`), or an index with nothing in
it yet (upload a document and watch the count).

`./setup.sh check` tests the key when one is set — it embeds a single word, so
a key that is missing, wrong, or not authorized on the project is a line there
rather than a question that never gets answered. No key is not an error: the
line reads `·` and everything except Ask still works.

## Two things the numbers will not tell you

**Consumer lag is not progress here.** A Flink Kafka source only commits
offsets back to its group on a completed checkpoint, and these jobs run without
checkpointing, so the group stays empty and a naive `end - committed` reports
the whole topic as a backlog forever. The dashboard shows *not reported*
instead of a wrong number; watch the input and output rates instead.

**CMF does not proxy Flink's REST API.** On the hosted system the JobManager is
inside the Kubernetes cluster, so there are no per-vertex counters: CMF gives
the job state and the cluster info, and stage activity is inferred from the
throughput of the topics around each stage. If you have a route to a
JobManager, point the dashboard at it and the per-vertex view comes back:

```bash
FLINK_REST_URL=http://localhost:8081 ./run.sh
```

## Layout

```
app.py                        the Streamlit page: header, DAG, topics, messages, index, ask
inspector/settings.py         profiles, and pushing them into the environment
inspector/deployment.py       which of the two pipelines is deployed, asked of CMF
inspector/cos.py              the bucket's console URL, composed from its CRN
inspector/kafka_probe.py      topic sizes, group lag, reading messages
inspector/flink_probe.py      Flink REST and CMF
inspector/opensearch_probe.py index stats and search
inspector/rag.py              embed the question, retrieve, prompt, answer
inspector/topology.py         the stage graph of each job, from pipeline.config
inspector/render.py           DOT generation and record formatting
inspector/brand.py            the TechXchange header and the product logos
```

The logos and the conference artwork are not in here — they are in `assets/`
at the top of the repo, because the lab guide and the architecture diagrams
want the same files. `brand.py` inlines them as data URIs and picks the
reversed variant for the black header; a missing `assets/` folder degrades to
text rather than breaking the page. See `assets/README.md`.

`topology.py` is the file to edit when a stage is added to a job: it describes
`enrich_job` and `full_job` stage by stage, resolves the optional ones against
the live configuration (a stage turned off in `lab.yaml` is drawn greyed out), and
names each operator with the string the job passes to `.name()` — which is how
a stage is matched to a vertex in Flink's REST output. Its `_ingest_chain` is
the other half: the bucket and Docling drawn in front of the chunk topic, from
the same settings.

The probes deliberately reuse the repo's own code rather than restating it:
`labtools.kafka` for the connection, `pipeline.config` for the
topics and toggles, and `scripts/drain_topic.py` for summarising a record. A
change to the pipeline's configuration shows up here without a second edit.
