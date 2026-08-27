# docling-trigger

The thing that turns an upload into a pipeline run:

```
upload → IBM COS bucket → object-write event → this app (Code Engine)
       → Docling SaaS (convert + chunk) → the student's chunk topic → Flink
```

One deployment serves the whole class. It is deployed once, by whoever runs the
workshop, and every student then points their own bucket at it.

It is not part of the pipeline image and shares no code with it: no Flink, no
JVM, no tokenizer, no `docling` SDK. Three wheels and a ~158 MB image, of which
123 MB is the `python:3.12-slim` base.

## The idea: nothing per student lives here

A student needs three things to be true of their events, and all three are
different for every student:

| | |
|---|---|
| **their Docling instance** | each student has their own service URL and key |
| **their chunk topic** | `ws.07.chunks` — a shared topic would split partitions between students, not copy the data to each of them |
| **their bucket** | which the event itself names |

So they travel *with the event*, as CloudEvent extensions on the student's own
subscription, and the app keeps no student state at all: no config map, no
lookup table, no per-student route. Two events from two students differ only in
three header values. That is what makes "deploy once, works for thirty" true
rather than aspirational.

Everything that is the *same* for the class — the Kafka cluster, the COS
credentials, the chunker settings — is environment on the deployment.

## What a delivery gets back

| | |
|---|---|
| `202` | submitted — the body carries Docling's `task_id` |
| `200` `ignored` | a delete, or a file Docling does not convert, or an immediate redelivery. Deliberately *not* an error: a non-2xx would have the subscription retry it forever |
| `400` | the event names no object, or a required header is missing — the body says which one |
| `502` | Docling was unreachable or refused the submission. Nothing was submitted, so a redelivery is safe |

Add `X-Dry-Run: 1` to any POST to be told exactly what *would* be submitted,
secrets redacted, without submitting it. It is the fastest way to check a
student's headers.

## Fire and forget, and where exactly the "forget" is

The handler does one POST to Docling's async batch endpoint, gets a task id
back in well under a second, and answers the notification with it. It never
polls the task, never fetches a result, and never learns whether the conversion
worked — Docling writes the chunks to Kafka itself, so the evidence is messages
arriving on the topic.

What it deliberately does **not** do is answer `202` first and submit in a
background task afterwards. Code Engine is Knative underneath: once the
response is written and nothing is in flight, the instance can be frozen or
scaled to zero, and background work goes with it. Awaiting the submit keeps the
request alive for the ~100 ms that matters and buys a real status code — a
delivery that gets a 502 can be redelivered, whereas one that got a 202 and
then evaporated is a document that silently never arrives.

## How the object reaches Docling

Docling fetches the document itself; this app never downloads it. Two ways to
arrange that, `COS_SOURCE_MODE`:

- **`presigned` (default)** — the app signs a GET URL for exactly that object
  (SigV4, `docling_trigger/presign.py`, ~40 lines of `hashlib` rather than a
  boto3 dependency) and submits it as an ordinary HTTP source. The credentials
  never leave the app, the URL addresses one object, and it expires.
- **`s3`** — Docling reads the bucket itself, with the HMAC credentials in the
  request. Simpler, but note that its `key_prefix` is a *prefix*: a key of
  `report.pdf` also selects `report.pdf.bak`, which `max_num_elements: 1` caps
  rather than resolves.

The signature is pinned to AWS's published test vector in
`tests/test_trigger_presign.py`, because a signature is either byte-for-byte
right or it is a 403 from COS with no explanation.

## The broker's certificate

The lab brokers are signed by a private CA, so a hosted Docling has no reason
to trust them. The `kafka_chunks` target takes the CA inside its `auth` block,
base64-encoded:

```yaml
auth:
  kind: sasl
  mechanism: PLAIN
  username: kafka-admin
  password: "…"
  ca_cert: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0t…      # base64 of kafka-ca.crt
```

`setup.sh` reads `kafka-ca.crt` (or `KAFKA_CA_LOCATION`), encodes it and puts
it in the Code Engine secret, so the class runs with the brokers **verified**
rather than with verification switched off. Supplying a CA is what makes
verification possible, so it also turns `verify_certs` on — `KAFKA_VERIFY_CERTS`
still overrides that in either direction.

The value is validated at startup: a PEM is encoded, an already-base64 blob is
checked for decoding to a certificate, and anything else is reported on `/` and
`/health` rather than sent. A wrong CA otherwise surfaces as a TLS handshake
failure deep inside Docling that this app never sees.

`scripts/saas_ingest.py --ca-cert` sends the identical field, so the laptop
producer and the trigger build the same target.

## Handling the whole class at once

- **one shared `httpx.AsyncClient`**, so TLS handshakes and connections to the
  Docling hosts are reused across students and across events. A class shares a
  handful of hostnames, so after the first event nearly every submission goes
  down an already-open connection.
- **async, single process** (`WEB_CONCURRENCY=1`). Thirty simultaneous uploads
  are thirty coroutines waiting on the network, not thirty threads.
- **a semaphore** (`TRIGGER_MAX_CONCURRENCY`, 16) so a burst queues instead of
  stampeding whoever is on the other end.
- **a short retry** on 429/5xx, bounded, because a notification is a live
  request — and duplicate conversions are harmless anyway: `chunk_id` is
  derived from the file's bytes, so the pipeline collapses them.
- **a TTL set** that drops an obvious redelivery of the same object+etag. Best
  effort by construction — it is per instance, and there may be several. It
  saves a conversion; it is not what makes duplicates safe.

## Working on it

```bash
uv run --group trigger python -m docling_trigger.app          # :8080
curl -s localhost:8080                                        # the instructions page
curl -sS -X POST localhost:8080/ -H 'X-Dry-Run: 1' \
  -H 'X-Docling-Url: http://localhost:5001' -H 'X-Kafka-Topic: docling.chunks' \
  -d @tests/fixtures/cos-event.json
```

`tests/fixtures/cos-event.json` is a real notification body, captured from a
bucket subscription pointed at a request dumper.

```bash
uv run pytest -k trigger
```

`test_trigger_event.py`, `test_trigger_presign.py` and `test_trigger_request.py`
need nothing installed — the parsing, the signature and the payload are
standard-library only — and `test_trigger_app.py` skips itself where `fastapi`
is not present, exactly as the pyflink tests skip outside the image.

The image is `images/trigger.Dockerfile`, built from the project root: it
installs the `trigger` dependency group and copies `src/docling_trigger`, and
nothing else. Deploying it is the workshop's job, not a student's.
