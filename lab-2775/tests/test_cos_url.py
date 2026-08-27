"""Tests for the dashboard's COS console URL, composed from a bucket CRN.

``cos.py`` has no imports beyond the standard library, so this needs none of
the dashboard's dependencies to run.
"""

from inspector import cos

# A real bucket CRN and the console URL the console itself produces for it.
CRN = ("crn:v1:bluemix:public:cloud-object-storage:global:"
       "a/793a70a62275449e8baa35262f4e4d3c:e8db6702-e181-48df-8cf2-df4a55c59448:"
       "bucket:bucket-etftth-input-docs")
URL = ("https://cloud.ibm.com/objectstorage/"
       "crn%3Av1%3Abluemix%3Apublic%3Acloud-object-storage%3Aglobal%3A"
       "a%2F793a70a62275449e8baa35262f4e4d3c%3Ae8db6702-e181-48df-8cf2-df4a55c59448%3A%3A"
       "?paneId=bucket_overview&bucket=bucket-etftth-input-docs&bucketRegion=ca-tor"
       "&endpoint=s3.ca-tor.cloud-object-storage.appdomain.cloud")


def test_composed_url_matches_the_console_url_byte_for_byte():
    assert cos.bucket_url(CRN, region="ca-tor") == URL


def test_crn_splits_into_instance_and_bucket():
    instance, bucket = cos.parse_crn(CRN)
    # The console addresses the instance: same CRN, resource type and id emptied.
    assert instance.endswith("e8db6702-e181-48df-8cf2-df4a55c59448::")
    assert ":bucket:" not in instance
    assert bucket == "bucket-etftth-input-docs"


def test_an_instance_crn_parses_with_no_bucket():
    instance, bucket = cos.parse_crn(
        "crn:v1:bluemix:public:cloud-object-storage:global:a/acct:inst::"
    )
    assert bucket == ""
    assert instance.endswith("inst::")


def test_region_is_optional_and_only_costs_the_region_hints():
    # A CRN carries no region — a bucket is still addressable without one, so a
    # missing COS_BUCKET_REGION degrades the link rather than breaking it.
    url = cos.bucket_url(CRN)
    assert url.endswith("?paneId=bucket_overview&bucket=bucket-etftth-input-docs")
    assert "bucketRegion" not in url and "endpoint" not in url


def test_explicit_endpoint_and_bucket_win_over_the_derived_ones():
    url = cos.bucket_url(CRN, region="ca-tor", endpoint="s3.private.example",
                         bucket="other-bucket")
    assert "bucket=other-bucket" in url
    assert "endpoint=s3.private.example" in url


def test_garbage_is_rejected_rather_than_turned_into_a_broken_link():
    import pytest
    for bad in ("", "not-a-crn", "crn:v1:bluemix"):
        with pytest.raises(ValueError):
            cos.parse_crn(bad)
