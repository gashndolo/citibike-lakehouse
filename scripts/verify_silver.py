from pyspark.sql import functions as F

from lakehouse import config
from lakehouse.spark_session import get_spark
def main():
    spark = get_spark("verify_silver", driver_memory="3g")
    spark.conf.set("spark.sql.shuffle.partitions", "16")
    t = config.SILVER_TABLE
    df = spark.table(t)

    print("\n== Silver per month ==")
    (df.groupBy("src_month")
       .agg(F.count("*").alias("rows"),
            F.countDistinct("ride_id").alias("distinct_ride_ids"),
            F.sum(F.pmod(F.col("row_hash"), F.lit(2147483647))).alias("content_hash"))
       .orderBy("src_month").show(60, truncate=False))

    print("== Reconciliation: bronze = silver + rejected + duplicates removed ==")
    rec = spark.sql(f"""
        WITH b AS (SELECT src_month, COUNT(*) AS bronze_rows
                   FROM {config.BRONZE_TABLE} GROUP BY 1),
             s AS (SELECT src_month, COUNT(*) AS silver_rows FROM {t} GROUP BY 1),
             r AS (SELECT src_month, COUNT(*) AS rejected FROM {config.REJECTED_TABLE} GROUP BY 1),
             l AS (SELECT src_month, duplicates_removed FROM (
                     SELECT *, ROW_NUMBER() OVER (PARTITION BY src_month
                                                  ORDER BY loaded_at DESC) AS rn
                     FROM {config.SILVER_LOG_TABLE} WHERE status = 'success') WHERE rn = 1)
        SELECT b.src_month, b.bronze_rows,
               COALESCE(s.silver_rows, 0) AS silver_rows,
               COALESCE(r.rejected, 0) AS rejected,
               COALESCE(l.duplicates_removed, 0) AS duplicates,
               b.bronze_rows - COALESCE(s.silver_rows, 0) - COALESCE(r.rejected, 0)
                             - COALESCE(l.duplicates_removed, 0) AS unexplained
        FROM b LEFT JOIN s USING (src_month) LEFT JOIN r USING (src_month)
               LEFT JOIN l USING (src_month)
        ORDER BY 1""")
    rec.show(60, truncate=False)
    bad = [x["src_month"] for x in rec.collect() if x["unexplained"] != 0]
    print("RECONCILIATION:", "OK, every bronze row is accounted for" if not bad
          else f"MISMATCH in {bad}")

    tot = spark.sql(f"""SELECT SUM(record_count) AS rows, COUNT(*) AS files,
        ROUND(SUM(file_size_in_bytes) / 1e6, 1) AS mb FROM {t}.data_files""").first()
    snaps = spark.sql(f"SELECT COUNT(*) AS n FROM {t}.snapshots").first()["n"]
    distinct_ids = df.select("ride_id").distinct().count()
    print(f"\nTOTAL rows={tot['rows']} files={tot['files']} size_mb={tot['mb']} snapshots={snaps}")
    print(f"distinct ride_id={distinct_ids} (equals rows: {distinct_ids == tot['rows']})")

    print("\n== Rejected by reason ==")
    spark.sql(f"""SELECT reject_reason, COUNT(*) AS rows
                  FROM {config.REJECTED_TABLE} GROUP BY 1 ORDER BY 2 DESC""").show(truncate=False)

    print("== Trip duration (seconds) ==")
    df.agg(F.min("duration_seconds").alias("min"), F.avg("duration_seconds").alias("avg"),
           F.expr("percentile_approx(duration_seconds, array(0.5, 0.95, 0.99))").alias("p50_p95_p99"),
           F.max("duration_seconds").alias("max"),
           F.count_if(F.col("duration_seconds") > 86400).alias("over_24h")).show(truncate=False)

    print("== Types and members ==")
    df.groupBy("rideable_type", "member_casual").count().orderBy("rideable_type", "member_casual").show()
    print("columns:", df.schema.simpleString())
    spark.stop()





if __name__ == "__main__":
    main()