"""A presigned GET URL for one COS object — AWS Signature V4, no SDK.

Docling fetches the document itself; this app never downloads it. So what has
to cross the wire is a URL Docling can GET, for exactly one object, that
carries its own authorisation and expires.

Why presign rather than hand Docling the S3 credentials (which it accepts, see
``submit.s3_source``):

* **it is exactly one object.** The S3 source addresses objects by *prefix*, so
  a key of ``report.pdf`` also selects ``report.pdf.bak``. A presigned URL has
  no such ambiguity.
* **the credentials stay here.** One HMAC key pair covers every student's
  bucket in the workshop's COS instance; it is deployment configuration, and it
  never leaves this process.
* **it is the ordinary HTTP source path** — the same one
  ``scripts/saas_ingest.py`` uses for arXiv URLs, and the one with the fewest
  moving parts on the service side.

Path-style addressing (``https://<endpoint>/<bucket>/<key>``) is used rather
than virtual-host style because it is what IBM COS documents and it survives
bucket names that virtual-host style cannot express.

Forty lines of hashlib rather than a boto3 dependency: the algorithm is fixed,
it is tested against AWS's published test vector in
``tests/test_trigger_presign.py``, and it keeps the image at "python + a web
server".
"""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
from urllib.parse import quote

ALGORITHM = "AWS4-HMAC-SHA256"
SERVICE = "s3"
# S3 presigned URLs sign the request without hashing a body — there is none.
UNSIGNED = "UNSIGNED-PAYLOAD"


def endpoint_for(region: str) -> str:
    """The public S3 endpoint of a COS region — the console's own default.

    Same formula as ``dashboard/inspector/cos.py``; repeated rather than
    imported, because this app is deployed on its own and shares no code with
    the laptop-side tools.
    """
    return f"s3.{region}.cloud-object-storage.appdomain.cloud" if region else ""


def region_for(endpoint: str) -> str:
    """The region inside a COS endpoint host, e.g. ``s3.ca-tor.cloud-...`` -> ``ca-tor``.

    The signature's credential scope has to name the bucket's region, and the
    endpoint already carries it — so a deployment that sets ``COS_ENDPOINT``
    does not also have to set ``COS_BUCKET_REGION``. Private and direct
    endpoints (``s3.direct.ca-tor...``, ``s3.private.ca-tor...``) put a
    qualifier in front of the region, which is skipped.
    """
    parts = [p for p in endpoint.split(".") if p]
    if len(parts) < 2 or parts[0] != "s3":
        return ""
    candidate = parts[1]
    if candidate in ("direct", "private") and len(parts) > 2:
        candidate = parts[2]
    return "" if candidate.startswith("cloud-object-storage") else candidate


def _sign(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()


def _signing_key(secret: str, datestamp: str, region: str) -> bytes:
    key = _sign(f"AWS4{secret}".encode(), datestamp)
    key = _sign(key, region)
    key = _sign(key, SERVICE)
    return _sign(key, "aws4_request")


def sign_query(
    *,
    host: str,
    path: str,
    access_key: str,
    secret_key: str,
    region: str,
    expires: int = 3600,
    now: dt.datetime | None = None,
) -> str:
    """The signed query string for a GET of ``path`` on ``host``.

    Split out from :func:`presigned_get` so the signature can be checked against
    AWS's published presigned-URL test vector, which is addressed
    virtual-host style and so cannot be produced by the path-style composer.
    """
    now = now or dt.datetime.now(tz=dt.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = now.strftime("%Y%m%d")
    scope = f"{datestamp}/{region}/{SERVICE}/aws4_request"

    query = {
        "X-Amz-Algorithm": ALGORITHM,
        "X-Amz-Credential": f"{access_key}/{scope}",
        "X-Amz-Date": amz_date,
        "X-Amz-Expires": str(int(expires)),
        "X-Amz-SignedHeaders": "host",
    }
    # Canonical query: percent-encoded, sorted by key — which for these five
    # names the alphabet already gives.
    canonical_query = "&".join(f"{k}={quote(v, safe='-_.~')}" for k, v in sorted(query.items()))
    canonical_request = "\n".join(
        [
            "GET",
            path,
            canonical_query,
            f"host:{host}\n",
            "host",
            UNSIGNED,
        ]
    )
    string_to_sign = "\n".join(
        [
            ALGORITHM,
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )
    signature = hmac.new(
        _signing_key(secret_key, datestamp, region), string_to_sign.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return f"{canonical_query}&X-Amz-Signature={signature}"


def object_path(bucket: str, key: str) -> str:
    """The path-style canonical URI of an object.

    ``quote`` leaves the separators between key segments alone and
    percent-encodes everything else — a space in an object name included.
    """
    return "/" + quote(f"{bucket}/{key.lstrip('/')}", safe="/")


def presigned_get(
    *,
    endpoint: str,
    bucket: str,
    key: str,
    access_key: str,
    secret_key: str,
    region: str = "",
    expires: int = 3600,
    secure: bool = True,
    now: dt.datetime | None = None,
) -> str:
    """A URL that GETs ``bucket/key`` for ``expires`` seconds, and nothing else.

    ``now`` exists so the signature is reproducible in a test; leave it unset.
    """
    if not (access_key and secret_key):
        raise ValueError("COS credentials are not configured (COS_ACCESS_KEY_ID / COS_SECRET_ACCESS_KEY)")
    if not endpoint:
        raise ValueError("no COS endpoint (set COS_ENDPOINT or COS_BUCKET_REGION)")
    region = region or region_for(endpoint)
    if not region:
        raise ValueError(f"cannot tell the region from endpoint {endpoint!r} — set COS_BUCKET_REGION")

    path = object_path(bucket, key)
    signed = sign_query(
        host=endpoint,
        path=path,
        access_key=access_key,
        secret_key=secret_key,
        region=region,
        expires=expires,
        now=now,
    )
    scheme = "https" if secure else "http"
    return f"{scheme}://{endpoint}{path}?{signed}"
