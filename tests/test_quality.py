import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "glue"))
from quality import validate_record  # noqa: E402


BASE = {
    "trip_id": "trip-1", "trip_start_timestamp": "2023-04-01T12:30:00",
    "trip_miles": "3.5", "fare": "12.25",
}


def test_valid_record_and_partition():
    result, reason = validate_record(BASE)
    assert reason is None
    assert result == {
        "trip_id": "trip-1", "trip_start_timestamp": "2023-04-01 12:30:00",
        "trip_date": "2023-04-01", "trip_miles": 3.5, "fare": 12.25,
    }


@pytest.mark.parametrize("changed,reason", [
    ({"trip_id": None}, "missing_required_field"),
    ({"trip_miles": " "}, "missing_required_field"),
    ({"trip_start_timestamp": "not-a-date"}, "invalid_timestamp"),
    ({"trip_start_timestamp": "2000-01-01T01:00:00"}, "timestamp_out_of_range"),
    ({"trip_miles": "3x"}, "invalid_number"),
    ({"trip_miles": "NaN"}, "value_out_of_range"),
    ({"fare": "Infinity"}, "value_out_of_range"),
    ({"trip_miles": "-0.1"}, "value_out_of_range"),
    ({"fare": "10001"}, "value_out_of_range"),
])
def test_invalid_values(changed, reason):
    _, actual = validate_record({**BASE, **changed})
    assert actual == reason


def test_missing_column_rejected_without_crash():
    row = {key: value for key, value in BASE.items() if key != "fare"}
    assert validate_record(row) == (None, "schema_mismatch")


def test_boundary_values_and_whitespace():
    clean, reason = validate_record({**BASE, "trip_id": " id ", "trip_miles": "0", "fare": "10000"})
    assert reason is None
    assert clean["trip_id"] == "id"
    assert clean["fare"] == 10000
