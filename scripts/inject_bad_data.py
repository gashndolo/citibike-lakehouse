from pyspark import StorageLevel
from pyspark.sql import functions as F

from lakehouse import config, quality
from lakehouse.spark_session import get_spark


def main():
    spark = get_spark("inject_bad_data", driver_memory="3g")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    clean = spark.table(config.SILVER_TABLE).where("src_month = '202102'")

    def hit(seed, p):
        return F.rand(seed) < p

    bad = (clean
           .withColumn("duration_seconds",
                       F.when(hit(1, 0.03), F.lit(90000)).otherwise(F.col("duration_seconds")))
           .withColumn("member_casual",
                       F.when(hit(2, 0.02), F.lit("unknown")).otherwise(F.col("member_casual")))
           .withColumn("start_lat",
                       F.when(hit(3, 0.02), F.lit(0.0)).otherwise(F.col("start_lat")))
           .withColumn("ride_id",
                       F.when(hit(4, 0.01), F.lit("DUPLICATE0000")).otherwise(F.col("ride_id")))
           .withColumn("start_station_id",
                       F.when(hit(5, 0.05), F.lit(None).cast("string"))
                        .otherwise(F.col("start_station_id")))
           ).persist(StorageLevel.MEMORY_AND_DISK)

    print("\nInjected: 3% trips of 25h, 2% member_casual='unknown', 2% start_lat=0, "
          "1% ride_id duplicated, 5% start_station_id null")

    print("\n===== CLEAN February (dry run) =====")
    clean_vr = quality.validate(clean)
    quality.print_results(quality.parse_results(clean_vr, "202102"))
    print("gate would:", "PASS" if clean_vr.success else "FAIL")

    print("\n===== CORRUPTED February (dry run) =====")
    bad_vr = quality.validate(bad)
    rows = quality.parse_results(bad_vr, "202102")
    quality.print_results(rows)
    failed = [f"{r[1]}:{r[2]}" for r in rows if not r[4]]
    print("gate would:", "PASS" if bad_vr.success else f"FAIL ({len(failed)} checks failed)")

    print("\n== Rows the quarantine would catch in the corrupted data ==")
    quality.row_violations(bad).groupBy("rule").count().show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()