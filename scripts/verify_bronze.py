from pyspark.sql import functions as F

from lakehouse import config
from lakehouse.spark_session import get_spark


def main():
    spark = get_spark("verify_bronze", driver_memory="2g")
    t = config.BRONZE_TABLE
    df = spark.table(t)

    # ingested_at differs on every load, so it is left out of the content hash
    data_cols = [c for c in df.columns if c != "ingested_at"]
    content_hash = F.sum(F.pmod(F.xxhash64(*[F.col(c) for c in data_cols]), F.lit(2147483647)))

    print("\n== Bronze per month ==")
    (df.groupBy("src_month")
       .agg(F.count("*").alias("rows"),
            F.countDistinct("src_file").alias("parts"),
            F.count("corrupt_record").alias("corrupt_rows"),
            content_hash.alias("content_hash"))
       .orderBy("src_month").show(60, truncate=False))

    print("== Data files per month ==")
    spark.sql(f"""
        SELECT partition.src_month AS src_month, COUNT(*) AS files,
               ROUND(SUM(file_size_in_bytes) / 1e6, 1) AS mb
        FROM {t}.data_files GROUP BY 1 ORDER BY 1""").show(60, truncate=False)

    tot = spark.sql(f"""
        SELECT SUM(record_count) AS rows, COUNT(*) AS files,
               ROUND(SUM(file_size_in_bytes) / 1e6, 1) AS mb FROM {t}.data_files""").first()
    snaps = spark.sql(f"SELECT COUNT(*) AS n FROM {t}.snapshots").first()["n"]
    print(f"TOTAL rows={tot['rows']} files={tot['files']} size_mb={tot['mb']} snapshots={snaps}")
    print("columns:", df.columns)

    print("\n== Latest load log entries ==")
    spark.sql(f"""
        SELECT src_month, rows_loaded, rows_replaced, corrupt_rows, schema_added,
               columns_missing, ROUND(seconds, 1) AS seconds, loaded_at
        FROM {config.LOAD_LOG_TABLE} ORDER BY loaded_at DESC LIMIT 15""").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()