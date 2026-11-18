"""Glue 4.0 Spark ETL: validate bounded CSV input and publish snapshot + audit summary."""

import json
import sys
from datetime import datetime, timezone

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, Window, functions as F, types as T

from quality import REQUIRED_COLUMNS, validate_record


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_BUCKET", "CURATED_BUCKET", "REJECTED_BUCKET"])
    spark = SparkSession.builder.getOrCreate()
    raw_path = f"s3://{args['RAW_BUCKET']}/trips/"
    raw = spark.read.option("header", "true").option("mode", "PERMISSIVE").csv(raw_path)
    if not set(REQUIRED_COLUMNS).issubset(raw.columns):
        raise ValueError(f"Input CSV must contain columns: {', '.join(REQUIRED_COLUMNS)}")

    shape = T.StructType([
        T.StructField("trip_id", T.StringType()),
        T.StructField("trip_start_timestamp", T.StringType()),
        T.StructField("trip_date", T.StringType()),
        T.StructField("trip_miles", T.DoubleType()),
        T.StructField("fare", T.DoubleType()),
        T.StructField("reason", T.StringType()),
    ])

    @F.udf(shape)
    def check(trip_id, trip_start_timestamp, trip_miles, fare):
        clean, reason = validate_record(dict(zip(REQUIRED_COLUMNS, (trip_id, trip_start_timestamp, trip_miles, fare))))
        if reason:
            return (None, None, None, None, None, reason)
        return (clean["trip_id"], clean["trip_start_timestamp"], clean["trip_date"],
                clean["trip_miles"], clean["fare"], None)

    records = raw.select(
        *[F.col(column) for column in REQUIRED_COLUMNS],
        F.input_file_name().alias("source_file"),
        F.to_json(F.struct(*[F.col(column) for column in REQUIRED_COLUMNS])).alias("raw_record"),
    ).withColumn("checked", check(*[F.col(column) for column in REQUIRED_COLUMNS])).cache()
    processed = records.count()
    invalid = records.where(F.col("checked.reason").isNotNull())
    valid = records.where(F.col("checked.reason").isNull())
    ranked = valid.withColumn("rank", F.row_number().over(
        Window.partitionBy("checked.trip_id").orderBy("source_file", "raw_record")
    )).cache()
    duplicate_rows = ranked.where(F.col("rank") > 1)
    accepted_rows = ranked.where(F.col("rank") == 1)
    invalid_count = invalid.count()
    duplicate_count = duplicate_rows.count()
    accepted_count = processed - invalid_count - duplicate_count

    accepted = accepted_rows.select(
        F.col("checked.trip_id").alias("trip_id"),
        F.to_timestamp(F.col("checked.trip_start_timestamp")).alias("trip_start_timestamp"),
        F.col("checked.trip_miles").alias("trip_miles"),
        F.col("checked.fare").alias("fare"),
        F.col("checked.trip_date").alias("trip_date"),
    )
    rejected = invalid.select(
        F.col("trip_id"), F.col("checked.reason").alias("reason"), "raw_record", "source_file"
    ).unionByName(duplicate_rows.select(
        F.col("trip_id"), F.lit("duplicate_trip_id").alias("reason"), "raw_record", "source_file"
    ))
    accepted.write.mode("overwrite").partitionBy("trip_date").parquet(f"s3://{args['CURATED_BUCKET']}/trips/")
    rejected.write.mode("overwrite").parquet(f"s3://{args['REJECTED_BUCKET']}/trips/")

    generated = datetime.now(timezone.utc)
    run_id = generated.strftime("%Y%m%dT%H%M%S%fZ")
    summary = {
        "run_id": run_id, "generated_at": generated.isoformat(), "processed": processed,
        "accepted": accepted_count, "invalid": invalid_count, "duplicates": duplicate_count,
        "rejected": invalid_count + duplicate_count,
        "rejection_rate": (invalid_count + duplicate_count) / processed if processed else 0.0,
    }
    boto3.client("s3").put_object(
        Bucket=args["CURATED_BUCKET"], Key=f"quality/summary/{run_id}.json",
        Body=(json.dumps(summary) + "\n").encode("utf-8"), ContentType="application/json",
        ServerSideEncryption="AES256",
    )
    print(json.dumps(summary))
    ranked.unpersist()
    records.unpersist()


if __name__ == "__main__":
    main()
