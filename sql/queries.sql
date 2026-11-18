-- Use Athena workgroup data-lake-quality-dev (or -prod), database data_lake_quality_dev (or _prod).
-- Partition projection expects ISO trip_date filters for efficient scans.
SELECT trip_date, COUNT(*) AS trips, ROUND(AVG(fare), 2) AS avg_fare
FROM trips
WHERE trip_date BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY trip_date
ORDER BY trip_date DESC
LIMIT 30;

-- Detailed data quality summary; each successful run publishes one JSON object.
SELECT run_id, generated_at, processed, accepted, invalid, duplicates,
       rejected, ROUND(rejection_rate * 100, 2) AS rejected_pct
FROM quality_reports
ORDER BY generated_at DESC
LIMIT 10;

-- Sample rejections, including duplicate keys; may contain trip identifiers.
SELECT reason, COUNT(*) AS affected_rows
FROM rejected_trips
GROUP BY reason
ORDER BY affected_rows DESC;
