"""The Code Engine app: a COS upload event in, a Docling task out.

    upload → COS bucket → event notification → **this app** → Docling SaaS
           → kafka_chunks target → the student's chunk topic → Flink

One deployment serves the whole class. It holds no per-student state: the three
things that differ between students arrive as headers on their own bucket's
notification (``settings.JobSpec``), and everything else — the Kafka cluster,
the COS credentials, the chunker settings — is the same for everyone and comes
from the environment.

**Fire and forget, and where exactly the "forget" is.** The handler does one
HTTP POST to Docling's async batch endpoint, which returns a task id in well
under a second, and answers the notification with that id. It never polls
``/v1/status``, never fetches a result, and never learns whether the conversion
succeeded — Docling writes the chunks to Kafka itself, so the evidence of
success is messages arriving on the topic.

What it deliberately does *not* do is return 202 first and submit afterwards in
a background task. Code Engine is Knative underneath: once the response is
written and no request is in flight, the instance can be frozen or scaled to
zero, and background work vanishes with it. Awaiting the submit keeps the
request alive for the ~100 ms that matters, and buys a real error code for a
rejected submission — a delivery that gets a 502 can be retried by the
subscription, whereas one that got 202 and then died is silently lost.

**Concurrency.** Thirty students uploading at once is thirty concurrent
requests, each of which is almost entirely waiting on someone else's network.
So: one async worker (no thread pool, no forked workers by default), one shared
``httpx.AsyncClient`` so TLS handshakes and connections to the Docling hosts are
reused across students and across events, and a semaphore that bounds how many
submissions are in flight at once so a burst queues instead of stampeding.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from . import cosevent, submit
from .settings import (
    E_DOCLING_KEY,
    E_DOCLING_URL,
    E_STUDENT,
    E_TOPIC,
    H_DOCLING_KEY,
    H_DOCLING_URL,
    H_STUDENT,
    H_TOPIC,
    Env,
    JobSpec,
)

log = logging.getLogger("docling-trigger")

# Retry only what is worth retrying: the submission is idempotent in the sense
# that matters (a duplicate conversion produces identical chunk ids, which the
# pipeline collapses), so a transient refusal is better retried here than left
# to the subscription's much slower redelivery.
RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
RETRIES = int(os.environ.get("TRIGGER_SUBMIT_RETRIES", "2"))


class Recent:
    """A tiny TTL set that suppresses an immediate redelivery of one event.

    Best effort by construction: it is per instance, and Code Engine may be
    running several. That is fine — it is not the correctness mechanism. Chunk
    ids are derived from the file's bytes, so a document converted twice
    produces the same ids and the pipeline's dedup stage collapses them. This
    just stops the obvious duplicate from costing a second conversion.
    """

    def __init__(self, ttl: float, limit: int = 4096) -> None:
        self.ttl, self.limit, self._seen = ttl, limit, {}

    def hit(self, key: str, now: float | None = None) -> bool:
        # `now or time.monotonic()` would be wrong: a monotonic clock can read 0.
        now = time.monotonic() if now is None else now
        if self.ttl <= 0:
            return False
        deadline = self._seen.get(key)
        if deadline and deadline > now:
            return True
        if len(self._seen) >= self.limit:
            self._seen = {k: v for k, v in self._seen.items() if v > now}
            if len(self._seen) >= self.limit:  # still full: all of it is live
                self._seen.clear()
        self._seen[key] = now + self.ttl
        return False


@asynccontextmanager
async def lifespan(app: FastAPI):
    env = Env.from_environ()
    app.state.env = env
    app.state.gate = asyncio.Semaphore(env.max_concurrency)
    app.state.recent = Recent(env.dedup_ttl_s)
    app.state.client = httpx.AsyncClient(
        timeout=httpx.Timeout(env.submit_timeout_s, connect=10.0),
        # Keep-alive to the Docling hosts is the whole efficiency story: a
        # class shares a handful of hostnames, so after the first event nearly
        # every submission reuses an open TLS connection.
        limits=httpx.Limits(max_connections=env.max_concurrency * 2, max_keepalive_connections=32),
        follow_redirects=True,
    )
    for problem in env.problems():
        log.warning("configuration: %s", problem)
    log.info(
        "ready: mode=%s bootstrap=%s endpoint=%s",
        env.source_mode,
        ",".join(env.bootstrap) or "<unset>",
        env.cos_endpoint or env.cos_region or "<unset>",
    )
    try:
        yield
    finally:
        await app.state.client.aclose()


app = FastAPI(
    title="docling-trigger",
    summary="COS upload events -> Docling SaaS conversion jobs",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict[str, Any]:
    """Liveness. Deliberately 200 even when misconfigured — see ``/``."""
    env: Env = app.state.env
    return {"status": "ok", "mode": env.source_mode, "problems": env.problems()}


@app.get("/", response_class=PlainTextResponse)
@app.get("/help", response_class=PlainTextResponse)
async def usage() -> str:
    """What a student has to configure, served from the app they configure it on."""
    env: Env = app.state.env
    problems = env.problems()
    endpoint_note = f"<from region {env.cos_region or '?'}>"
    cert_note = "verified against the configured CA" if env.kafka_ca_cert else "not verified (no CA configured)"
    return "\n".join(
        [
            "docling-trigger — IBM COS upload events into Docling SaaS.",
            "",
            "Subscribe this app to your bucket's write events and set three values on the",
            "subscription — as CloudEvent extensions (what `ibmcloud ce sub cos create`",
            "takes) or as plain HTTP headers. Either spelling works:",
            "",
            f"  --extension {E_DOCLING_URL + '=<url>':<28} or  {H_DOCLING_URL}: <url>",
            f"  --extension {E_DOCLING_KEY + '=<key>':<28} or  {H_DOCLING_KEY}: <key>",
            f"  --extension {E_TOPIC + '=<topic>':<28} or  {H_TOPIC}: <topic>",
            f"  --extension {E_STUDENT + '=<id>':<28} or  {H_STUDENT}: <id>   (optional, labels the logs)",
            "",
            "  e.g.  doclingurl=https://api.dcls.saas.ibm.com   chunkstopic=ws.07.chunks",
            "",
            "Then upload a document to the bucket. Nothing else starts the pipeline.",
            "",
            f"Kafka (shared, from the deployment): {','.join(env.bootstrap) or '<unset>'}",
            f"Broker certificate:                  {cert_note}",
            f"COS endpoint (shared):               {env.cos_endpoint or endpoint_note}",
            f"Source mode:                         {env.source_mode}",
            f"Chunker:                             hybrid, {env.tokenizer}, {env.max_tokens} tokens",
            "",
            ("Configuration problems:\n  " + "\n  ".join(problems)) if problems else "Configuration: ok.",
            "",
            "POST any path here with a COS notification body to trigger a conversion;",
            "add 'X-Dry-Run: 1' to be told what would be submitted without submitting it.",
        ]
    )


@app.post("/{path:path}")
async def notify(request: Request) -> JSONResponse:
    """Handle one COS event notification.

    Every path is accepted on purpose: subscriptions in the wild post to ``/``,
    to ``/callback``, to ``/events``, and a workshop should not turn a path
    typo into a silent 404 nobody looks at.
    """
    env: Env = app.state.env
    started = time.monotonic()

    try:
        body = json.loads(await request.body() or b"{}")
    except json.JSONDecodeError as exc:
        return _reply(400, {"error": f"body is not JSON: {exc}"})

    try:
        event = cosevent.parse(body, request.headers)
    except ValueError as exc:
        return _reply(400, {"error": str(exc)})

    # 200, not 4xx: these are healthy deliveries this app has nothing to do
    # with, and a non-2xx would have the subscription retry them forever.
    if not event.is_write:
        return _reply(200, {"ignored": "not an object write", "operation": event.operation, "key": event.key})
    if not cosevent.accepts(event.key, env.suffixes):
        return _reply(200, {"ignored": "not a convertible document", "key": event.key})

    # A structured CloudEvent carries the extensions as top-level attributes
    # rather than Ce-* headers, so the body is offered as a second source.
    spec = JobSpec.from_headers(request.headers, env, attributes=body if isinstance(body, dict) else None)
    problems = spec.problems()
    if problems:
        _log(event, spec, "rejected", problems=problems)
        return _reply(400, {"error": "missing configuration", "problems": problems})

    try:
        payload = submit.build_request(env, spec, event)
    except ValueError as exc:  # unusable COS configuration — the app's fault
        _log(event, spec, "misconfigured", error=str(exc))
        return _reply(500, {"error": str(exc), "problems": env.problems()})

    # Answered before the dedup check, and deliberately: a dry run must leave
    # no trace, or checking a student's headers would suppress the very upload
    # they check it with.
    if request.headers.get("x-dry-run", "").strip().lower() in ("1", "true", "yes"):
        return _reply(200, {"dry_run": True, "request": submit.redact(payload), "spec": spec.redacted()})

    if app.state.recent.hit(event.dedup_key()):
        _log(event, spec, "duplicate")
        return _reply(200, {"ignored": "duplicate delivery", "key": event.key})

    try:
        task_id, status = await _post(spec, payload)
    except httpx.HTTPError as exc:
        _log(event, spec, "unreachable", error=repr(exc), ms=_ms(started))
        # 502 so the subscription can redeliver: nothing was submitted.
        return _reply(502, {"error": f"Docling did not answer: {exc}", "url": submit.submit_url(spec.docling_url)})
    except _Rejected as exc:
        _log(event, spec, "rejected-by-docling", error=exc.detail, ms=_ms(started))
        return _reply(502, {"error": "Docling rejected the submission", "status": exc.status, "detail": exc.detail})

    _log(event, spec, "submitted", task_id=task_id, task_status=status, ms=_ms(started))
    return _reply(
        202,
        {
            "submitted": True,
            "task_id": task_id,
            "task_status": status,
            "bucket": event.bucket,
            "key": event.key,
            "topic": spec.topic,
        },
    )


class _Rejected(Exception):
    def __init__(self, status: int, detail: str) -> None:
        super().__init__(detail)
        self.status, self.detail = status, detail


async def _post(spec: JobSpec, payload: dict[str, Any]) -> tuple[str, str]:
    """Submit, with a short retry on the failures that are worth retrying."""
    client: httpx.AsyncClient = app.state.client
    url = submit.submit_url(spec.docling_url)
    last: _Rejected | None = None

    async with app.state.gate:
        for attempt in range(RETRIES + 1):
            try:
                reply = await client.post(url, json=payload, headers=submit.auth_headers(spec))
            except httpx.HTTPError:
                if attempt == RETRIES:
                    raise
            else:
                if reply.status_code == 200:
                    body = reply.json()
                    return str(body.get("task_id", "")), str(body.get("task_status", ""))
                detail = reply.text[:500]
                last = _Rejected(reply.status_code, detail)
                if reply.status_code not in RETRY_STATUS or attempt == RETRIES:
                    raise last
            # 0.4s, 0.8s — bounded, because a COS notification is a live request.
            await asyncio.sleep(0.4 * (2**attempt))
    raise last or _Rejected(0, "no attempt was made")  # unreachable


def _reply(status: int, body: dict[str, Any]) -> JSONResponse:
    return JSONResponse(status_code=status, content=body)


def _ms(started: float) -> int:
    return int((time.monotonic() - started) * 1000)


def _log(event: cosevent.ObjectEvent, spec: JobSpec, outcome: str, **extra: Any) -> None:
    """One JSON line per event — the only trace of a student's upload there is."""
    record = {
        "outcome": outcome,
        "student": spec.student,
        "bucket": event.bucket,
        "key": event.key,
        "bytes": event.length,
        "topic": spec.topic,
        "docling": spec.docling_url,
        **extra,
    }
    level = logging.INFO if outcome in ("submitted", "duplicate") else logging.WARNING
    log.log(level, json.dumps({k: v for k, v in record.items() if v not in ("", None)}))


def main() -> None:
    """Entry point for the container: ``python -m docling_trigger.app``."""
    import uvicorn

    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(), format="%(levelname)s %(message)s")
    uvicorn.run(
        "docling_trigger.app:app",
        host="0.0.0.0",  # the container's port; Code Engine publishes it
        port=int(os.environ.get("PORT", "8080")),
        workers=int(os.environ.get("WEB_CONCURRENCY", "1")),
        access_log=False,  # the handler's own JSON line says more
    )


if __name__ == "__main__":
    main()
