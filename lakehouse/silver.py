import argparse
import logging
import time
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from lakehouse import bronze, config
from lakehouse.spark_session import get_spark

log = logging.getLogger("silver")

RAW_COLS = ["ride_id", "rideable_type", "started_at", "ended_at",
            "start_station_name", "start_station_id", "end_station_name", "end_station_id",
            "start_lat", "start_lng", "end_lat", "end_lng", "member_casual"]
VALUE_COLS = ["ride_id", "rideable_type", "started_at", "ended_at", "duration_seconds",
              "start_station_name", "start_station_id", "end_station_name", "end_station_id",
              "start_lat", "start_lng", "end_lat", "end_lng", "member_casual"]
SILVER_COLS = VALUE_COLS + ["row_hash", "src_month", "src_file", "ingested_at"]

SILVER_DDL = f"""
CREATE TABLE IF NOT EXISTS {config.SILVER_TABLE} (
    ride_id STRING, rideable_type STRING,
    started_at TIMESTAMP_NTZ, ended_at TIMESTAMP_NTZ, duration_seconds BIGINT,
    start_station_name STRING, start_station_id STRING,
    end_station_name STRING, end_station_id STRING,
    start_lat DOUBLE, start_lng DOUBLE, end_lat DOUBLE, end_lng DOUBLE,
    member_casual STRING, row_hash BIGINT,
    src_month STRING, src_file STRING, ingested_at TIMESTAMP
) USING iceberg PARTITIONED BY (src_month)
"""
REJECTED_DDL = f"""
CREATE TABLE IF NOT EXISTS {config.REJECTED_TABLE} (
    src_month STRING, src_file STRING, reject_reason STRING, ride_id STRING,
    started_raw STRING, ended_raw STRING, corrupt_record STRING
) USING iceberg PARTITIONED BY (src_month)
"""
LOG_DDL = f"""
CREATE TABLE IF NOT EXISTS {config.SILVER_LOG_TABLE} (
    src_month STRING, bronze_fingerprint STRING, rows_in BIGINT, rows_rejected BIGINT,
    duplicates_removed BIGINT, rows_silver BIGINT, inserted BIGINT, updated BIGINT,
    unchanged BIGINT, snapshot_id BIGINT, seconds DOUBLE, loaded_at TIMESTAMP, status STRING
) USING iceberg
"""
LOG_SCHEMA = ("src_month string, bronze_fingerprint string, rows_in bigint, rows_rejected bigint, "
              "duplicates_removed bigint, rows_silver bigint, inserted bigint, updated bigint, "
              "unchanged bigint, snapshot_id bigint, seconds double, loaded_at timestamp, "
              "status string")


def _clean(name: str):
    """Trimmed string; empty becomes NULL."""
    t = F.trim(F.col(name))
    return F.when(F.length(t) > 0, t)


def classify(bronze_df: DataFrame) -> DataFrame:
    """Type the columns and add reject_reason (NULL = row is good) and row_hash."""
    clean = bronze_df.select("src_month", "src_file", "ingested_at", "corrupt_record",
                             *[_clean(c).alias(c) for c in RAW_COLS])
    typed = clean.select(
        "src_month", "src_file", "ingested_at", "corrupt_record", "ride_id",
        F.lower("rideable_type").alias("rideable_type"),
        F.col("started_at").alias("started_raw"),
        F.col("ended_at").alias("ended_raw"),
        F.col("started_at").try_cast("timestamp_ntz").alias("started_at"),
        F.col("ended_at").try_cast("timestamp_ntz").alias("ended_at"),
        "start_station_name", "start_station_id", "end_station_name", "end_station_id",
        *[F.col(c).try_cast("double").alias(c)
          for c in ("start_lat", "start_lng", "end_lat", "end_lng")],
        F.lower("member_casual").alias("member_casual"))
    typed = typed.withColumn("duration_seconds",
                             F.expr("timestampdiff(SECOND, started_at, ended_at)"))
    reason = (F.when(F.col("corrupt_record").isNotNull(), "corrupt_record")
              .when(F.col("ride_id").isNull(), "missing_ride_id")
              .when(F.col("started_at").isNull(), "bad_started_at")
              .when(F.col("ended_at").isNull(), "bad_ended_at")
              .when(F.col("ended_at") < F.col("started_at"), "ended_before_started"))
    return (typed.withColumn("reject_reason", reason)
                 .withColumn("row_hash", F.xxhash64(*[F.col(c) for c in VALUE_COLS])))


def dedupe_latest(df: DataFrame) -> DataFrame:
    """One row per ride_id: the most recently ingested copy wins."""
    w = Window.partitionBy("ride_id").orderBy(F.col("ingested_at").desc(),
                                              F.col("src_file").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")


def last_bronze_fingerprint_loaded(spark, month: str):
    rows = spark.sql(
        f"SELECT bronze_fingerprint FROM {config.SILVER_LOG_TABLE} "
        f"WHERE src_month = '{month}' AND status = 'success' "
        "ORDER BY loaded_at DESC LIMIT 1").collect()
    return rows[0]["bronze_fingerprint"] if rows else None


def load_month(spark, month: str, fingerprint: str):
    t0 = time.time()
    cls = classify(spark.table(config.BRONZE_TABLE).where(F.col("src_month") == month)
                   ).persist(StorageLevel.MEMORY_AND_DISK)
    good = None
    try:
        n = cls.agg(F.count("*").alias("n"),
                    F.count_if(F.col("reject_reason").isNotNull()).alias("rej")).first()
        rows_in, rejected = int(n["n"]), int(n["rej"])
        if rows_in == 0:
            raise ValueError(f"{month}: no bronze rows found")

        good = (dedupe_latest(cls.where(F.col("reject_reason").isNull()))
                .select(*SILVER_COLS).persist(StorageLevel.MEMORY_AND_DISK))
        rows_silver = good.count()
        dups = rows_in - rejected - rows_silver

        # Atomic replace of this month's rejects (also correct when there are none)
        (cls.where(F.col("reject_reason").isNotNull())
            .select("src_month", "src_file", "reject_reason", "ride_id",
                    "started_raw", "ended_raw", "corrupt_record")
            .writeTo(config.REJECTED_TABLE).overwrite(F.col("src_month") == month))

        target = (spark.table(config.SILVER_TABLE).where(F.col("src_month") == month)
                  .select("ride_id", F.col("row_hash").alias("t_hash")))
        st = (good.select("ride_id", "row_hash").join(target, "ride_id", "left")
              .agg(F.count_if(F.col("t_hash").isNull()).alias("new"),
                   F.count_if(F.col("t_hash").isNotNull()
                              & (F.col("t_hash") != F.col("row_hash"))).alias("changed"))
              .first())
        inserted, updated = int(st["new"]), int(st["changed"])
        unchanged = rows_silver - inserted - updated

        good.createOrReplaceTempView("silver_src")
        spark.sql(f"""
            MERGE INTO {config.SILVER_TABLE} t
            USING silver_src s
            ON t.src_month = '{month}' AND t.ride_id = s.ride_id
            WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""")

        after = (spark.table(config.SILVER_TABLE)
                 .where(F.col("src_month") == month).count())
        if after != rows_silver:
            raise RuntimeError(
                f"{month}: silver has {after} rows but {rows_silver} were expected. "
                "Rows removed from the source are not deleted by this MERGE.")
    finally:
        cls.unpersist()
        if good is not None:
            good.unpersist()

    snap = spark.sql(f"SELECT snapshot_id FROM {config.SILVER_TABLE}.snapshots "
                     "ORDER BY committed_at DESC LIMIT 1").first()["snapshot_id"]
    seconds = time.time() - t0
    row = (month, fingerprint, rows_in, rejected, dups, rows_silver, inserted, updated,
           unchanged, int(snap), seconds, datetime.now(timezone.utc), "success")
    spark.createDataFrame([row], LOG_SCHEMA).writeTo(config.SILVER_LOG_TABLE).append()
    log.info("%s: in=%s rejected=%s duplicates=%s silver=%s inserted=%s updated=%s "
             "unchanged=%s in %.1fs", month, f"{rows_in:,}", f"{rejected:,}", f"{dups:,}",
             f"{rows_silver:,}", f"{inserted:,}", f"{updated:,}", f"{unchanged:,}", seconds)


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(description="Build the silver table from bronze")
    p.add_argument("--months", nargs="*", help="YYYYMM values (default: all loaded in bronze)")
    p.add_argument("--force", action="store_true", help="reprocess even if bronze is unchanged")
    args = p.parse_args()

    spark = get_spark("silver_load")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.SILVER_NS}")
        for ddl in (SILVER_DDL, REJECTED_DDL, LOG_DDL):
            spark.sql(ddl)
        available = [r[0] for r in spark.sql(
            f"SELECT DISTINCT src_month FROM {config.LOAD_LOG_TABLE} "
            "WHERE status = 'success' ORDER BY 1").collect()]
        months = sorted(args.months) if args.months else available
        unknown = [m for m in months if m not in available]
        if unknown:
            raise SystemExit(f"Not loaded in bronze: {unknown}")
        for month in months:
            fp = bronze.last_fingerprint(spark, month)
            if not args.force and last_bronze_fingerprint_loaded(spark, month) == fp:
                log.info("%s: skip (bronze unchanged since the last silver load)", month)
                continue
            load_month(spark, month, fp)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()