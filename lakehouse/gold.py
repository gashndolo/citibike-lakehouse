import argparse
import logging
import time
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse import config
from lakehouse.spark_session import get_spark

log = logging.getLogger("gold")

DAILY_DDL = f"""
CREATE TABLE IF NOT EXISTS {config.DAILY_TABLE} (
    src_month STRING, ride_date DATE, rideable_type STRING, member_casual STRING,
    rides BIGINT, duration_sum_seconds BIGINT
) USING iceberg PARTITIONED BY (src_month)
"""
STATION_DDL = f"""
CREATE TABLE IF NOT EXISTS {config.STATION_TABLE} (
    src_month STRING, start_station_id STRING, start_station_name STRING,
    rides BIGINT, duration_sum_seconds BIGINT
) USING iceberg PARTITIONED BY (src_month)
"""
LOG_DDL = f"""
CREATE TABLE IF NOT EXISTS {config.GOLD_LOG_TABLE} (
    src_month STRING, silver_loaded_at TIMESTAMP, rides_in BIGINT, daily_rows BIGINT,
    station_rows BIGINT, seconds DOUBLE, loaded_at TIMESTAMP, status STRING
) USING iceberg
"""
LOG_SCHEMA = ("src_month string, silver_loaded_at timestamp, rides_in bigint, daily_rows bigint, "
              "station_rows bigint, seconds double, loaded_at timestamp, status string")


def exclude_quarantined(silver: DataFrame, quarantined: DataFrame) -> DataFrame:
    return silver.join(quarantined.select("ride_id").distinct(), "ride_id", "left_anti")


def build_daily(df: DataFrame) -> DataFrame:
    return (df.groupBy("src_month", F.to_date("started_at").alias("ride_date"),
                       "rideable_type", "member_casual")
              .agg(F.count("*").alias("rides"),
                   F.sum("duration_seconds").alias("duration_sum_seconds")))


def build_station(df: DataFrame) -> DataFrame:
    return (df.where(F.col("start_station_id").isNotNull())
              .groupBy("src_month", "start_station_id")
              .agg(F.max("start_station_name").alias("start_station_name"),
                   F.count("*").alias("rides"),
                   F.sum("duration_seconds").alias("duration_sum_seconds")))


def gate_decision(spark, month: str):
    """Return silver_loaded_at if the month may be built, else None (with the reason logged)."""
    row = spark.sql(f"SELECT status, silver_loaded_at FROM {config.GATE_STATUS_TABLE} "
                    f"WHERE src_month = '{month}'").first()
    if row is None:
        log.error("%s: BLOCKED, no quality gate result", month)
        return None
    if row["status"] != "pass":
        log.error("%s: BLOCKED, gate status is '%s'", month, row["status"])
        return None
    latest = spark.sql(f"SELECT MAX(loaded_at) AS t FROM {config.SILVER_LOG_TABLE} "
                       f"WHERE src_month = '{month}' AND status = 'success'").first()["t"]
    if row["silver_loaded_at"] != latest:
        log.error("%s: BLOCKED, gate result is stale (silver was reloaded after the gate ran)",
                  month)
        return None
    return latest


def last_built(spark, month: str):
    rows = spark.sql(f"SELECT silver_loaded_at FROM {config.GOLD_LOG_TABLE} "
                     f"WHERE src_month = '{month}' AND status = 'success' "
                     "ORDER BY loaded_at DESC LIMIT 1").collect()
    return rows[0]["silver_loaded_at"] if rows else None


def load_month(spark, month: str, silver_loaded_at):
    t0 = time.time()
    silver = spark.table(config.SILVER_TABLE).where(F.col("src_month") == month)
    quarantined = spark.table(config.QUARANTINE_TABLE).where(F.col("src_month") == month)
    trusted = exclude_quarantined(silver, quarantined).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        rides_in = trusted.count()
        # coalesce(1): these results are tiny, so one file per month avoids small files
        build_daily(trusted).coalesce(1).writeTo(config.DAILY_TABLE).overwrite(
            F.col("src_month") == month)
        build_station(trusted).coalesce(1).writeTo(config.STATION_TABLE).overwrite(
            F.col("src_month") == month)
    finally:
        trusted.unpersist()
    daily_rows = spark.table(config.DAILY_TABLE).where(F.col("src_month") == month).count()
    station_rows = spark.table(config.STATION_TABLE).where(F.col("src_month") == month).count()
    seconds = time.time() - t0
    spark.createDataFrame(
        [(month, silver_loaded_at, rides_in, daily_rows, station_rows, seconds,
          datetime.now(timezone.utc), "success")], LOG_SCHEMA
    ).writeTo(config.GOLD_LOG_TABLE).append()
    log.info("%s: built | trusted rides=%s daily_rows=%s station_rows=%s | %.1fs", month,
             f"{rides_in:,}", f"{daily_rows:,}", f"{station_rows:,}", seconds)


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(description="Build gold tables from trusted silver months")
    p.add_argument("--months", nargs="*", help="YYYYMM values (default: all gated months)")
    p.add_argument("--force", action="store_true", help="rebuild even if silver is unchanged")
    args = p.parse_args()

    spark = get_spark("gold_load")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    blocked = []
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.GOLD_NS}")
        for ddl in (DAILY_DDL, STATION_DDL, LOG_DDL):
            spark.sql(ddl)
        available = [r[0] for r in spark.sql(
            f"SELECT src_month FROM {config.GATE_STATUS_TABLE} ORDER BY 1").collect()]
        months = sorted(args.months) if args.months else available
        for month in months:
            loaded_at = gate_decision(spark, month)       # the gate is always checked first
            if loaded_at is None:
                blocked.append(month)
                continue
            if not args.force and last_built(spark, month) == loaded_at:
                log.info("%s: skip (silver unchanged since the last gold build)", month)
                continue
            load_month(spark, month, loaded_at)
    finally:
        spark.stop()
    if blocked:
        log.error("Blocked months: %s", blocked)
        raise SystemExit(1)


if __name__ == "__main__":
    main()