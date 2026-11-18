"""Spark-independent validation rules shared with the Glue job."""

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Mapping

REQUIRED_COLUMNS = ("trip_id", "trip_start_timestamp", "trip_miles", "fare")


def validate_record(row: Mapping[str, str | None]) -> tuple[dict[str, str | float] | None, str | None]:
    if any(column not in row for column in REQUIRED_COLUMNS):
        return None, "schema_mismatch"
    values = {column: (row[column] or "").strip() for column in REQUIRED_COLUMNS}
    if any(not value for value in values.values()):
        return None, "missing_required_field"
    try:
        timestamp = datetime.fromisoformat(values["trip_start_timestamp"])
    except ValueError:
        return None, "invalid_timestamp"
    if not (2010 <= timestamp.year <= 2099):
        return None, "timestamp_out_of_range"
    try:
        miles = Decimal(values["trip_miles"])
        fare = Decimal(values["fare"])
    except InvalidOperation:
        return None, "invalid_number"
    if not (miles.is_finite() and fare.is_finite() and 0 <= miles <= 1000 and 0 <= fare <= 10000):
        return None, "value_out_of_range"
    return {
        "trip_id": values["trip_id"],
        "trip_start_timestamp": timestamp.isoformat(sep=" "),
        "trip_date": timestamp.date().isoformat(),
        "trip_miles": float(miles),
        "fare": float(fare),
    }, None
