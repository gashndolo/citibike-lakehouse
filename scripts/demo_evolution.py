from pyspark.sql import functions as F
from lakehouse import config
from lakehouse.spark_session import get_spark

DEMO = f"{config.SILVER_NS}.evolution_demo"

def files(spark):
    return {r[0] for r in spark.sql(f"SELECT file_path FROM {DEMO}.files").collect()}
def main():
    spark = get_spark("demo_evolution", driver_memory="2g")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.sql(f"DROP TABLE IF EXISTS {DEMO} PURGE")
    spark.sql(f"CREATE TABLE {DEMO} USING iceberg PARTITIONED BY (src_month) AS "
              f"SELECT * FROM {config.SILVER_TABLE} WHERE src_month = '202102'")
    base = files(spark)
    rows0 = spark.table(DEMO).count()
    s0 = spark.sql(f"SELECT snapshot_id FROM {DEMO}.snapshots "
                   "ORDER BY committed_at DESC LIMIT 1").first()[0]
    print(f"\nBASELINE: {rows0:,} rows, {len(base)} data files, snapshot {s0}")

    print("\n== Schema evolution ==")
    spark.sql(f"ALTER TABLE {DEMO} ADD COLUMN trip_date DATE")
    spark.sql(f"ALTER TABLE {DEMO} RENAME COLUMN duration_seconds TO trip_seconds")
    after = files(spark)
    print(f"data files rewritten by the two schema changes: {len(after ^ base)}")
    spark.sql(f"SELECT ride_id, trip_seconds, trip_date FROM {DEMO} LIMIT 3").show(truncate=False)
    print("columns when reading the pre-change snapshot:",
          spark.sql(f"SELECT * FROM {DEMO} VERSION AS OF {s0}").columns)

    print("\n== Partition evolution ==")
    spark.sql(f"ALTER TABLE {DEMO} REPLACE PARTITION FIELD src_month WITH months(started_at)")
    new = (spark.table(config.SILVER_TABLE).where("src_month = '202103'")
           .withColumnRenamed("duration_seconds", "trip_seconds")
           .withColumn("trip_date", F.to_date("started_at")))
    new_rows = new.count()
    new.select(*spark.table(DEMO).columns).writeTo(DEMO).append()
    spark.sql(f"""SELECT spec_id, COUNT(*) AS files, SUM(record_count) AS rows
                  FROM {DEMO}.files GROUP BY spec_id ORDER BY spec_id""").show()
    total = spark.table(DEMO).count()
    print(f"total rows={total:,}, expected={rows0 + new_rows:,}, "
          f"{'OK' if total == rows0 + new_rows else 'MISMATCH'}")
    spark.sql(f"""SELECT date_format(started_at, 'yyyy-MM') AS month, COUNT(*) AS rides
                  FROM {DEMO} GROUP BY 1 ORDER BY 1""").show()

    spark.sql(f"DROP TABLE {DEMO} PURGE")
    print("demo table dropped")
    spark.stop()

if __name__ == "__main__":
    main()