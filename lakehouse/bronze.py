import argparse
import logging
import shutil
import time
from collections import defaultdict
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from lakehouse import config, landing
from lakehouse.spark_session import get_spark

log = logging.getLogger("bronze")

LOAD_LOG_DDL = f"""
CREATE TABLE IF NOT EXISTS {config.LOAD_LOG_TABLE} (
    src_month STRING, zip_name STRING, fingerprint STRING, members INT,
    source_bytes BIGINT, source_columns ARRAY<STRING>, schema_added STRING,
    columns_missing STRING, rows_loaded BIGINT, rows_replaced BIGINT,
    corrupt_rows BIGINT, snapshot_id BIGINT, seconds DOUBLE,
    loaded_at TIMESTAMP, status STRING
) USING iceberg
"""
LOAD_LOG_SCHEMA = (
    "src_month string, zip_name string, fingerprint string, members int, "
    "source_bytes bigint, source_columns array<string>, schema_added string, "
    "columns_missing string, rows_loaded bigint, rows_replaced bigint, "
    "corrupt_rows bigint, snapshot_id bigint, seconds double, "
    "loaded_at timestamp, status string")


def last_fingerprint(spark, month: str):
    rows = spark.sql(
        f"SELECT fingerprint FROM {config.LOAD_LOG_TABLE} "
        f"WHERE src_month = '{month}' AND status = 'success' "
        "ORDER BY loaded_at DESC LIMIT 1").collect()
    return rows[0]["fingerprint"] if rows else None


def read_month(spark, files, columns, month):
    schema = StructType([StructField(c, StringType(), True) for c in columns]
                        + [StructField("corrupt_record", StringType(), True)])
    raw = (spark.read.schema(schema)
           .option("header", "true")
           .option("mode", "PERMISSIVE")
           .option("columnNameOfCorruptRecord", "corrupt_record")
           .option("quote", '"').option("escape", '"')
           .csv([str(p) for p in files]))
    return raw.select(
        F.lit(month).alias("src_month"),
        F.col("_metadata.file_name").alias("src_file"),
        F.current_timestamp().alias("ingested_at"),
        *[F.col(c) for c in columns],
        F.col("corrupt_record"))


def align_to_table(spark, df):
    """Create the table on first use; add new columns; fill missing ones with NULL."""
    table = config.BRONZE_TABLE
    if not spark.catalog.tableExists(table):
        (df.limit(0).writeTo(table).using("iceberg")
           .partitionedBy(F.col("src_month"))
           .tableProperty("write.distribution-mode", "none")
           .create())
        log.info("Created %s", table)

    table_cols = [f.name for f in spark.table(table).schema.fields]
    added = [c for c in df.columns if c not in table_cols]
    for c in added:
        spark.sql(f"ALTER TABLE {table} ADD COLUMN `{c}` STRING")
        log.warning("Schema drift: added column %s (metadata-only change)", c)
    table_cols += added

    missing = [c for c in table_cols if c not in df.columns]
    for c in missing:
        df = df.withColumn(c, F.lit(None).cast("string"))
    return df.select(*table_cols), added, missing


def load_month(spark, zpath, month, members, fingerprint):
    t0 = time.time()
    outdir = config.EXTRACT_DIR / month
    shutil.rmtree(outdir, ignore_errors=True)
    outdir.mkdir(parents=True)
    try:
        files = landing.extract_month(zpath, members, outdir)
        headers = {tuple(landing.normalize_columns(landing.read_header_file(f))) for f in files}
        if len(headers) != 1:
            raise ValueError(f"{month}: the parts of one month have different headers: {headers}")
        columns = list(next(iter(headers)))
        df, added, missing = align_to_table(spark, read_month(spark, files, columns, month))

        log.info("%s: writing %d part file(s) to %s", month, len(files), config.BRONZE_TABLE)
        df.writeTo(config.BRONZE_TABLE).overwritePartitions()
    finally:
        shutil.rmtree(outdir, ignore_errors=True)

    snap = spark.sql(f"SELECT snapshot_id, summary FROM {config.BRONZE_TABLE}.snapshots "
                     "ORDER BY committed_at DESC LIMIT 1").first()
    loaded = int(snap["summary"].get("added-records", 0))
    replaced = int(snap["summary"].get("deleted-records", 0))
    corrupt = (spark.table(config.BRONZE_TABLE)
               .where(F.col("src_month") == month)
               .where(F.col("corrupt_record").isNotNull()).count())
    seconds = time.time() - t0

    row = (month, zpath.name, fingerprint, len(members), sum(m.size for m in members),
           columns, ",".join(added) or None, ",".join(missing) or None,
           loaded, replaced, corrupt, int(snap["snapshot_id"]), seconds,
           datetime.now(timezone.utc), "success")
    spark.createDataFrame([row], LOAD_LOG_SCHEMA).writeTo(config.LOAD_LOG_TABLE).append()

    log.info("%s: %s rows loaded (replaced %s) in %.1fs; corrupt=%s; parts=%d; "
             "new_columns=%s; missing_columns=%s", month, f"{loaded:,}", f"{replaced:,}",
             seconds, corrupt, len(members), added or "-", missing or "-")


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(description="Load Citi Bike months into the bronze Iceberg table")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--months", nargs="*", help="YYYYMM values (default: every month in the zip)")
    p.add_argument("--force", action="store_true", help="reload even if the source is unchanged")
    args = p.parse_args()

    zpath = landing.download_zip(args.year)
    by_month = defaultdict(list)
    for m in landing.list_members(zpath):
        by_month[m.month].append(m)
    wanted = sorted(args.months) if args.months else sorted(by_month)
    unknown = [m for m in wanted if m not in by_month]
    if unknown:
        raise SystemExit(f"Not in {zpath.name}: {unknown}")

    spark = get_spark("bronze_load")
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.BRONZE_NS}")
        spark.sql(LOAD_LOG_DDL)
        for month in wanted:
            fp = landing.month_fingerprint(by_month[month])
            if not args.force and last_fingerprint(spark, month) == fp:
                log.info("%s: skip (source unchanged since the last load)", month)
                continue
            load_month(spark, zpath, month, by_month[month], fp)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()