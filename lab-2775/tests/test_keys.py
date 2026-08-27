"""Tests for the key selectors.

They are three lines each and they are tested because of how they fail: a key
selector that is wrong does not raise, it sends related records to different
subtasks — dedup state stops matching, embed batches stop filling, and the job
stays green throughout.
"""

import json

import pytest

from pipeline.logic.keys import Shard, by_fingerprint


def rec(**fields) -> str:
    return json.dumps(fields)


def test_the_key_is_the_field():
    assert by_fingerprint(rec(fingerprint="abc")) == "abc"


def test_a_missing_key_is_the_empty_string_not_none():
    """Flink's STRING key type will not take None, and a null key would fail the
    job at the first record that lacks the field rather than at submit time."""
    assert by_fingerprint(rec(doc_id="d")) == ""
    assert by_fingerprint(rec(fingerprint=None)) == ""


def test_shard_is_stable_across_processes():
    """The point of crc32 over hash(): Python salts string hashing per process,
    so hash() gives the same record different keys in different TaskManagers."""
    value = rec(chunk_id="c-1")
    assert Shard(4)(value) == Shard(4)(value)
    # Recomputed from the algorithm, not from a previous run of this code.
    import zlib
    assert Shard(4)(value) == str(zlib.crc32(b"c-1") % 4)


def test_shard_spreads_over_the_range():
    keys = {Shard(4)(rec(chunk_id=f"c-{i}")) for i in range(50)}
    assert keys == {"0", "1", "2", "3"}


def test_shard_falls_back_to_the_document():
    assert Shard(4)(rec(doc_id="d")) == Shard(4)(rec(doc_id="d"))
    # A record with neither still keys, rather than failing: everything lands
    # on one shard, which is slow but not broken.
    assert Shard(4)(rec()) == Shard(4)(rec())


def test_shard_count_is_validated_at_construction():
    with pytest.raises(ValueError):
        Shard(0)
