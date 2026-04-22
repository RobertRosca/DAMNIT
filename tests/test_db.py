import numpy as np
import pytest

from damnit.backend.db import (BlobTypes, DamnitDB, ReducedData, ValueKind,
                                blob2complex, blob2numpy, complex2blob,
                                decode_value, encode_value, numpy2blob)


def test_bootstrap(pg_db):
    """A fresh DAMNIT DB has a persistent db_id and no proposals."""
    db = DamnitDB(proposal=None)
    assert isinstance(db.db_id, str) and len(db.db_id) > 0
    db.close()


def test_run_comment(mock_db):
    _, db = mock_db

    db.ensure_run(1234, 5, added_at=1670498578.)
    db.change_run_comment(1234, 5, 'Test comment')
    result = db.get_variable(1234, 5, "comment")
    assert result is not None
    value, _summary_type, _attrs = result
    assert value == "Test comment"


def test_standalone_comment(mock_db):
    _, db = mock_db

    ts = 1670498578.
    cid = db.add_standalone_comment(ts, 'Comment without run')
    db.change_standalone_comment(cid, 'Revised comment')
    res = db.list_standalone_comments()
    assert res == [(cid, ts, 'Revised comment')]


def test_tags(mock_db_with_data):
    _, db = mock_db_with_data

    # add_tag is a no-op in the Postgres schema - tags are attached to variables.
    assert db.add_tag("SPB") == "SPB"
    assert db.add_tag("SPB") == "SPB"  # idempotent
    assert db.get_tag_id("nonexistent") is None

    # Test getting tags for a variable
    var1_tags = db.get_variable_tags("scalar1")
    assert set(var1_tags) == {"scalar", "integer"}
    var2_tags = db.get_variable_tags("scalar2")
    assert set(var2_tags) == {"scalar", "float"}
    assert db.get_variable_tags("nonexistent_var") == []

    # Test getting variables by tag
    assert set(db.get_variables_by_tag("scalar")) == {"scalar1", "scalar2"}
    assert set(db.get_variables_by_tag("text")) == {"empty_string"}
    assert db.get_variables_by_tag("nonexistent") == []

    all_tags = set(db.get_all_tags())
    assert {"scalar", "integer", "float", "text"}.issubset(all_tags)

    # Test untagging variables
    db.untag_variable("scalar1", "scalar")
    assert set(db.get_variable_tags("scalar1")) == {"integer"}

    # untagging with nonexistent tag / variable should not raise
    db.untag_variable("scalar1", "nonexistent")
    assert set(db.get_variable_tags("scalar1")) == {"integer"}
    db.untag_variable("nonexistent_var", "important")


@pytest.mark.parametrize("value", [
    1+2j,
    0+0j,
    -1.5-3.7j,
    2.5+0j,
    0+3.1j,
    float('inf')+0j,
    complex(float('inf'), -float('inf')),
])
def test_complex_blob_conversion(value):
    blob = complex2blob(value)
    result = blob2complex(blob)
    assert result == value


def test_numpy_blob_conversion():
    arr = np.arange(6, dtype=np.float64).reshape(2, 3)
    blob = numpy2blob(arr)
    assert blob.startswith(b"\x93NUMPY")
    result = blob2numpy(blob)
    np.testing.assert_array_equal(result, arr)


def test_numpy_summary_type_and_storage(mock_db):
    _, db = mock_db
    db.ensure_run(1234, 1)
    arr = np.arange(10, dtype=np.float64)

    db.set_variable(1234, 1, "array", ReducedData(arr), provenance="test")
    value, summary_type, _attrs = db.get_variable(1234, 1, "array")
    assert summary_type == "numpy"
    np.testing.assert_array_equal(value, arr)

    db.set_variable(
        1234, 1, "line", ReducedData(arr, summary_type="trendline"), provenance="test"
    )
    value, summary_type, _attrs = db.get_variable(1234, 1, "line")
    assert summary_type == "trendline"
    np.testing.assert_array_equal(value, arr)


def test_encode_decode_roundtrip():
    # scalars
    for value in [None, True, False, 0, 42, -1, 3.14, "hello", ""]:
        enc = encode_value(ReducedData(value))
        decoded = decode_value(
            enc.kind, num=enc.num, text=enc.text,
            bool_=enc.bool_, bytes_=enc.bytes_,
        )
        assert decoded == value, f"Round-trip failed for {value!r}"

    # complex
    enc = encode_value(ReducedData(1 + 2j))
    assert enc.kind == ValueKind.complex_
    assert decode_value(enc.kind, bytes_=enc.bytes_) == (1 + 2j)

    # numpy
    arr = np.arange(6, dtype=np.float64).reshape(2, 3)
    enc = encode_value(ReducedData(arr))
    assert enc.kind == ValueKind.numpy
    np.testing.assert_array_equal(decode_value(enc.kind, bytes_=enc.bytes_), arr)


def test_set_variable_versioning(mock_db):
    _, db = mock_db
    db.ensure_run(1234, 1)
    db.set_variable(1234, 1, "foo", ReducedData(1), provenance="v1")
    db.set_variable(1234, 1, "foo", ReducedData(2), provenance="v2")

    # Only one current row per (proposal, run, name)
    from damnit.backend.db import RunVariable
    from sqlalchemy import select
    stmt = (
        select(RunVariable.version, RunVariable.value_num, RunVariable.is_current)
        .where(RunVariable.proposal == 1234)
        .where(RunVariable.run == 1)
        .where(RunVariable.name == "foo")
        .order_by(RunVariable.version)
    )
    rows = list(db.session.execute(stmt))
    assert [r.version for r in rows] == [1, 2]
    assert [r.is_current for r in rows] == [False, True]
    assert [r.value_num for r in rows] == [1.0, 2.0]
