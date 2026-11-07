from lakehouse.spark_session import get_spark


def main():
    spark = get_spark("smoke_iceberg", driver_memory="1g")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.smoke")
    spark.sql("DROP TABLE IF EXISTS lake.smoke.t PURGE")
    spark.sql("CREATE TABLE lake.smoke.t (id BIGINT, note STRING) USING iceberg")
    spark.sql("INSERT INTO lake.smoke.t VALUES (1, 'a'), (2, 'b')")
    spark.sql("INSERT INTO lake.smoke.t VALUES (3, 'c')")

    print("\nrows now:", spark.table("lake.smoke.t").count())
    snaps = spark.sql("""
        SELECT snapshot_id, operation, summary['added-records'] AS added_records
        FROM lake.smoke.t.snapshots ORDER BY committed_at""")
    snaps.show(truncate=False)

    first = snaps.first()["snapshot_id"]
    n = spark.sql(f"SELECT COUNT(*) AS c FROM lake.smoke.t VERSION AS OF {first}").first()["c"]
    print("rows at first snapshot (time travel):", n)

    spark.sql("DROP TABLE lake.smoke.t PURGE")
    spark.sql("DROP NAMESPACE lake.smoke")
    spark.stop()


if __name__ == "__main__":
    main()