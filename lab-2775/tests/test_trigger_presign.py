"""The presigned COS URL — checked against AWS's own published test vector.

A signature is either byte-for-byte right or it is a 403 from COS with no
explanation, so it is worth pinning to a vector rather than to itself. The one
here is the "GET Object" example from the Signature Version 4 query-parameter
documentation: it is virtual-host addressed, which is why ``sign_query`` takes
host and path rather than a bucket.
"""

from __future__ import annotations

import datetime as dt

import pytest

from docling_trigger import presign

VECTOR_SIGNATURE = "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"


def test_the_aws_test_vector_signs_byte_for_byte():
    query = presign.sign_query(
        host="examplebucket.s3.amazonaws.com",
        path="/test.txt",
        access_key="AKIAIOSFODNN7EXAMPLE",
        secret_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        region="us-east-1",
        expires=86400,
        now=dt.datetime(2013, 5, 24, tzinfo=dt.timezone.utc),
    )
    assert query.endswith(f"X-Amz-Signature={VECTOR_SIGNATURE}")
    assert "X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request" in query


def test_a_cos_url_is_path_style_and_carries_the_signature():
    url = presign.presigned_get(
        endpoint="s3.ca-tor.cloud-object-storage.appdomain.cloud",
        bucket="bucket-etftth-input-docs",
        key="inbox/1016445.pdf",
        access_key="ak",
        secret_key="sk",
        expires=900,
    )
    assert url.startswith(
        "https://s3.ca-tor.cloud-object-storage.appdomain.cloud/bucket-etftth-input-docs/inbox/1016445.pdf?"
    )
    assert "X-Amz-Expires=900" in url and "X-Amz-Signature=" in url
    # The region comes from the endpoint, so COS_BUCKET_REGION is optional.
    assert "%2Fca-tor%2Fs3%2F" in url


def test_a_space_in_the_object_name_is_encoded_once():
    url = presign.presigned_get(
        endpoint="s3.ca-tor.cloud-object-storage.appdomain.cloud",
        bucket="b",
        key="quarterly report.pdf",
        access_key="ak",
        secret_key="sk",
    )
    assert "/b/quarterly%20report.pdf?" in url


def test_the_region_is_read_out_of_any_endpoint_flavour():
    assert presign.region_for("s3.ca-tor.cloud-object-storage.appdomain.cloud") == "ca-tor"
    assert presign.region_for("s3.direct.us-south.cloud-object-storage.appdomain.cloud") == "us-south"
    assert presign.region_for("s3.private.eu-de.cloud-object-storage.appdomain.cloud") == "eu-de"
    assert presign.region_for("") == ""


def test_missing_configuration_is_an_error_not_a_broken_url():
    with pytest.raises(ValueError):
        presign.presigned_get(endpoint="s3.ca-tor.x", bucket="b", key="k", access_key="", secret_key="")
    with pytest.raises(ValueError):
        presign.presigned_get(endpoint="", bucket="b", key="k", access_key="a", secret_key="s")
    with pytest.raises(ValueError):  # an endpoint no region can be read from
        presign.presigned_get(endpoint="cos.example.com", bucket="b", key="k", access_key="a", secret_key="s")
