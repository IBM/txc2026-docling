"""Thin client for the Docling SaaS async conversion API — upload a local file.

Laptop-side only, like :mod:`producer.chunking`: neither Flink job converts
anything.

The service follows the docling-serve v1 contract:

    POST /v1/convert/file/async     -> {"task_id": ..., "task_status": "pending"}
    GET  /v1/status/poll/{task_id}  -> {"task_status": "pending|started|success|failure", ...}
    GET  /v1/result/{task_id}       -> {"document": {"json_content": {<DoclingDocument>}, ...}, ...}

Authentication is via the ``X-Api-Key`` header.

See https://developer.dcls.saas.ibm.com/ for the full reference.
"""

from __future__ import annotations

import time
from typing import Any

import requests


class DoclingConversionError(RuntimeError):
    pass


class DoclingClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        poll_interval_s: float = 2.0,
        timeout_s: float = 600.0,
        do_ocr: bool = True,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._poll_interval = poll_interval_s
        self._timeout = timeout_s
        self._do_ocr = do_ocr
        self._session = requests.Session()
        self._session.headers.update({"X-Api-Key": api_key, "Accept": "application/json"})

    def convert_file(self, path: str) -> dict[str, Any]:
        """Upload a local file (``POST /v1/convert/file/async``) and return the
        DoclingDocument as a dict.

        The one caller is ``scripts/ingest_folder.py --via serve``, which pushes
        local files to the SaaS endpoint rather than converting on the laptop.
        Documents that *are* reachable by URL never come through here: Docling's
        own ``kafka_chunks`` target writes them straight to the topic
        (``scripts/saas_ingest.py``).
        """
        import os

        with open(path, "rb") as fh:
            resp = self._session.post(
                f"{self._base}/v1/convert/file/async",
                files={"files": (os.path.basename(path), fh, "application/octet-stream")},
                # Options travel as flat form fields on the multipart request.
                data={
                    "to_formats": "json",
                    "do_ocr": str(self._do_ocr).lower(),
                    "image_export_mode": "placeholder",
                },
                timeout=120,
            )
        if resp.status_code not in (200, 202):
            raise DoclingConversionError(
                f"upload failed for {path!r}: HTTP {resp.status_code}: {resp.text[:500]}"
            )
        task_id = resp.json().get("task_id")
        if not task_id:
            raise DoclingConversionError(f"upload response missing task_id: {resp.text[:500]}")
        self._await_completion(task_id)
        return self._fetch_document(task_id)

    def _await_completion(self, task_id: str) -> None:
        deadline = time.monotonic() + self._timeout
        while True:
            resp = self._session.get(f"{self._base}/v1/status/poll/{task_id}", timeout=30)
            resp.raise_for_status()
            status = resp.json().get("task_status")
            if status == "success":
                return
            if status == "failure":
                raise DoclingConversionError(f"conversion task {task_id} failed")
            if time.monotonic() > deadline:
                raise DoclingConversionError(
                    f"conversion task {task_id} timed out after {self._timeout}s (last status={status})"
                )
            time.sleep(self._poll_interval)

    def _fetch_document(self, task_id: str) -> dict[str, Any]:
        resp = self._session.get(f"{self._base}/v1/result/{task_id}", timeout=60)
        resp.raise_for_status()
        payload = resp.json()
        document = payload.get("document") or {}
        json_content = document.get("json_content")
        if not json_content:
            raise DoclingConversionError(
                f"result for task {task_id} has no json_content (status={payload.get('status')})"
            )
        return json_content
