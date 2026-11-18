#!/usr/bin/env python3
"""Download a bounded public Chicago taxi CSV sample and upload to the raw zone."""

import argparse
import csv
import io
import subprocess
import urllib.parse
import urllib.request

SOURCE = "https://data.cityofchicago.org/resource/wrvz-psew.csv"
FIELDS = {"trip_id", "trip_start_timestamp", "trip_miles", "fare"}
MAX_BYTES = 2_000_000


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True, help="Raw bucket name from CDK output")
    parser.add_argument("--limit", type=int, default=500, help="Rows to request (1–1000)")
    args = parser.parse_args()
    if not 1 <= args.limit <= 1000:
        parser.error("--limit must be between 1 and 1000")
    query = urllib.parse.urlencode({
        "$select": "trip_id,trip_start_timestamp,trip_miles,fare",
        "$where": "trip_id IS NOT NULL",
        "$order": "trip_start_timestamp DESC,trip_id ASC",
        "$limit": args.limit,
    })
    with urllib.request.urlopen(f"{SOURCE}?{query}", timeout=30) as response:
        payload = response.read(MAX_BYTES + 1)
    if len(payload) > MAX_BYTES:
        raise ValueError("Sample exceeds 2 MB cap; refusing upload")
    csv_reader = csv.DictReader(io.StringIO(payload.decode("utf-8-sig")))
    if csv_reader.fieldnames is None or set(csv_reader.fieldnames) != FIELDS or not any(csv_reader):
        raise ValueError("Public CSV is empty or its schema has changed; refusing upload")
    destination = f"s3://{args.bucket}/trips/sample.csv"
    subprocess.run(["aws", "s3", "cp", "-", destination, "--sse", "AES256"], input=payload, check=True)
    print(f"Uploaded bounded sample to {destination}")


if __name__ == "__main__":
    main()
