"""The bucket's console URL, composed from the one string a student can copy.

The link that matters in this workshop is the bucket's page in the IBM Cloud
console, and it is not a nice URL::

    https://cloud.ibm.com/objectstorage/crn%3Av1%3Abluemix%3Apublic%3Acloud-object
    -storage%3Aglobal%3Aa%2F793a...%3Ae8db...%3A%3A?paneId=bucket_overview&bucket=
    bucket-etftth-input-docs&bucketRegion=ca-tor&endpoint=s3.ca-tor.cloud-object-
    storage.appdomain.cloud

Asking thirty people to paste *that* into a config file is asking for thirty
truncated URLs. But every part of it except the region is already inside the
bucket's CRN, which the console offers as a copy button::

    crn:v1:bluemix:public:cloud-object-storage:global:a/793a...:e8db...:bucket:bucket-etftth-input-docs
    └──────────────── the instance, percent-encoded into the path ──────────┘ └─── the bucket name ───┘

So the configuration is the CRN plus the bucket's region, and this module puts
the URL back together. A full ``COS_BUCKET_URL`` still wins over it — a lab that
hands out a link should not have to be reverse-engineered into a CRN.

Pure string handling, no imports beyond the standard library: it is unit-tested
in ``tests/test_cos_url.py`` rather than by clicking.
"""

from __future__ import annotations

from urllib.parse import quote

CONSOLE = "https://cloud.ibm.com/objectstorage"

# A COS CRN, split on ":" — ``a/<account>`` keeps its slash, which is why the
# path segment is percent-encoded rather than joined verbatim.
#   0   1  2       3      4                    5      6          7          8       9
#   crn:v1:bluemix:public:cloud-object-storage:global:a/<account>:<instance>:bucket:<name>
_SCOPE, _INSTANCE, _TYPE, _NAME = 6, 7, 8, 9


def parse_crn(crn: str) -> tuple[str, str]:
    """``(instance CRN, bucket name)`` from a bucket or instance CRN.

    The console addresses the *instance* — the CRN with its resource type and
    id emptied, ending in ``::`` — and names the bucket in the query string, so
    both halves are needed. An instance CRN (no ``:bucket:<name>`` tail) parses
    fine and yields an empty bucket name.
    """
    parts = [p for p in crn.strip().split(":")]
    if len(parts) < _INSTANCE + 1 or parts[0] != "crn":
        raise ValueError(f"not a CRN: {crn!r}")
    instance = ":".join([*parts[:_INSTANCE + 1], "", ""])
    bucket = parts[_NAME] if len(parts) > _NAME and parts[_TYPE] == "bucket" else ""
    return instance, bucket


def endpoint_for(region: str) -> str:
    """The public S3 endpoint of a bucket region — the console's own default."""
    return f"s3.{region}.cloud-object-storage.appdomain.cloud" if region else ""


def bucket_url(crn: str, region: str = "", endpoint: str = "", bucket: str = "") -> str:
    """The bucket's overview page in the IBM Cloud console.

    ``region`` is the only thing the CRN does not carry (a COS instance is
    ``global``; its buckets are not). Without it the link still opens the right
    bucket — the console falls back to looking the location up — so a missing
    region degrades the URL rather than invalidating it.
    """
    instance, crn_bucket = parse_crn(crn)
    bucket = bucket or crn_bucket
    query = ["paneId=bucket_overview"]
    if bucket:
        query.append(f"bucket={quote(bucket, safe='')}")
    if region:
        query.append(f"bucketRegion={quote(region, safe='')}")
    resolved = endpoint or endpoint_for(region)
    if resolved:
        query.append(f"endpoint={quote(resolved, safe='')}")
    return f"{CONSOLE}/{quote(instance, safe='')}?{'&'.join(query)}"
