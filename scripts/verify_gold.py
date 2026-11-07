from lakehouse import config
from lakehouse.spark_session import get_spark

def main():
    spark = get_spark("verify_gold", driver_memory="3g")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    d, st = config.DAILY_TABLE, config.STATION_TABLE

    print("\n== Reconciliation: silver - quarantined = gold daily; daily - null stations = station table ==")
    rec = spark.sql(f"""
        WITH s AS (SELECT src_month, COUNT(*) AS silver_rows
                   FROM {config.SILVER_TABLE} GROUP BY 1),
             q AS (SELECT src_month, COUNT(DISTINCT ride_id) AS quarantined
                   FROM {config.QUARANTINE_TABLE} GROUP BY 1),
             g AS (SELECT src_month, SUM(rides) AS daily_rides FROM {d} GROUP BY 1),
             t AS (SELECT src_month, SUM(rides) AS station_rides FROM {st} GROUP BY 1),
             n AS (SELECT s.src_month, COUNT(*) AS null_station
                   FROM {config.SILVER_TABLE} s
                   LEFT ANTI JOIN (SELECT DISTINCT src_month, ride_id
                                   FROM {config.QUARANTINE_TABLE}) q
                     ON s.ride_id = q.ride_id AND s.src_month = q.src_month
                   WHERE s.start_station_id IS NULL GROUP BY 1)
        SELECT s.src_month, silver_rows, COALESCE(quarantined, 0) AS quarantined,
               COALESCE(daily_rides, 0) AS daily_rides, COALESCE(station_rides, 0) AS station_rides,
               silver_rows - COALESCE(quarantined, 0) - COALESCE(daily_rides, 0) AS unexplained_daily,
               COALESCE(daily_rides, 0) - COALESCE(station_rides, 0)
                 - COALESCE(n.null_station, 0) AS unexplained_station
        FROM s LEFT JOIN q USING (src_month) LEFT JOIN g USING (src_month)
               LEFT JOIN t USING (src_month) LEFT JOIN n USING (src_month)
        ORDER BY 1""")
    rec.show(60, truncate=False)
    bad = [r["src_month"] for r in rec.collect()
           if r["unexplained_daily"] != 0 or r["unexplained_station"] != 0]
    print("RECONCILIATION:", "OK" if not bad else f"MISMATCH in {bad}")

    for t in (d, st):
        s = spark.sql(f"""SELECT COUNT(*) AS files, ROUND(SUM(file_size_in_bytes) / 1e6, 2) AS mb,
                          SUM(record_count) AS rows FROM {t}.data_files""").first()
        snaps = spark.sql(f"SELECT COUNT(*) AS n FROM {t}.snapshots").first()["n"]
        print(f"{t}: rows={s['rows']} files={s['files']} size_mb={s['mb']} snapshots={snaps}")

    print("\n== Rides by month (as the file month) and electric share ==")
    spark.sql(f"""SELECT src_month, SUM(rides) AS rides,
                  ROUND(100 * SUM(CASE WHEN rideable_type = 'electric_bike' THEN rides END)
                        / SUM(rides), 1) AS electric_pct,
                  ROUND(SUM(duration_sum_seconds) / SUM(rides) / 60, 1) AS avg_minutes
                  FROM {d} GROUP BY 1 ORDER BY 1""").show(20, truncate=False)

    print("== Top 10 start stations, whole year ==")
    spark.sql(f"""SELECT start_station_id, MAX(start_station_name) AS name, SUM(rides) AS rides,
                  ROUND(SUM(duration_sum_seconds) / SUM(rides) / 60, 1) AS avg_minutes
                  FROM {st} GROUP BY 1 ORDER BY 3 DESC LIMIT 10""").show(truncate=False)
    spark.stop()






if __name__ == "__main__":
    main()