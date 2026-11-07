# Metrics log

Every number measured during the build goes here, with how it was measured.
These become the README results and the resume bullets.

| Date | Phase | Metric | Value | How measured |
|------|-------|--------|-------|--------------|
| 2026-11-07 | 0 | Iceberg smoke test: commits | 2 append commits (2 and 1 records added); 3 rows in the table | `scripts/smoke_iceberg.py`, snapshots metadata table |
| 2026-11-07 | 0 | Iceberg smoke test: time travel | Reading the table at the first snapshot returned 2 rows (current table: 3 rows) | `SELECT ... VERSION AS OF <snapshot_id>` |
| 2026-11-07 | 0 | Great Expectations + pandas | Works | `scripts/smoke_gx.py` |
| 2026-11-07 | 0 | Great Expectations + Spark 4.0.1 | Works, so no fallback tool is needed | `scripts/smoke_gx.py` |
| 2026-11-07 | 0 | GX detects bad data (Spark mode) | On a 3-row DataFrame with 1 null `ride_id` and 1 negative duration: not-null check `success=False, unexpected=1`; range check `success=False, unexpected=1` | `scripts/smoke_gx.py` |
| 2026-11-07 | 0 | Source packaging | Citi Bike publishes yearly zips (`YYYY-citibike-tripdata.zip`) for 2020 to 2022, not monthly files | `scripts/list_source_files.py --prefix 2020 2021 2022` |
| 2026-11-07 | 0 | Source size, 2020 zip | 751.2 MB compressed | `scripts/list_source_files.py` |
| 2026-11-07 | 0 | Source size, 2021 zip | 1,074.4 MB compressed | `scripts/list_source_files.py` |
| 2026-11-07 | 0 | Source size, 2022 zip | 1,231.4 MB compressed | `scripts/list_source_files.py` |
| 2026-11-07 | 0 | Source size, 2020 to 2022 total | 3.06 GB compressed (uncompressed size not yet measured) | sum of the three zips above |
| 2026-11-07 | 0 | Source last-modified dates | 2020: 2024-07-31; 2021: 2024-08-13; 2022: 2024-07-31 (all re-uploaded in mid-2024) | `scripts/list_source_files.py` |
| 2026-11-07 | 0 | Versions | pyspark 4.0.1, Iceberg runtime 4.0_2.13 1.20.1, GX 1.24.1, Java 17.0.20.1 | pip freeze, .env, java -version |


du -sh data/warehouse - 917M
Metric	Value
Rows in bronze	27,130,122 (12 months of 2021)
Corrupt rows	0
Size	5.22 GB of CSV became 902 MB of Parquet (5.8x smaller), 54 files
Load time	about 4.3 min wall clock for the 11 new months; 7.5 s (Feb) to 32.1 s (Sep) per month
Throughput	about 100k rows/s end to end on a laptop (27.1M rows over about 276 s of load time)
Biggest month	202109, 3,219,934 rows, 107.1 MB
Snapshots	14 (1 table creation, 12 monthly loads, 1 forced reload)
Metric	Value
Full 2021 bronze load	27,130,122 rows, 54 files, 902.0 MB (from 5.22 GB CSV, 5.8x smaller), 14 snapshots, 0 corrupt rows
Crash test 202103	kill -9 during forced reload: rows 1,499,917, hash 1610617686640456 and snapshot count (14) unchanged; no load_log entry written; next run skipped

