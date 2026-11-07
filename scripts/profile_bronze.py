from pyspark.sql import functions as F

from lakehouse import config
from lakehouse.spark_session import get_spark


def main():
    spark = get_spark("profile_bronze", driver_memory="3g")
    spark.conf.set("spark.sql.shuffle.partitions", "16")
    df = spark.table(config.BRONZE_TABLE)
    data_cols = [c for c in df.columns
                 if c not in ("src_month", "src_file", "ingested_at", "corrupt_record")]

    s = F.expr("try_cast(started_at as timestamp_ntz)")
    e = F.expr("try_cast(ended_at as timestamp_ntz)")
    dur = F.expr("timestampdiff(SECOND, try_cast(started_at as timestamp_ntz), "
                 "try_cast(ended_at as timestamp_ntz))")
    la = F.expr("try_cast(start_lat as double)")
    lo = F.expr("try_cast(start_lng as double)")

    checks = {
        "rows": F.count("*"),
        "corrupt_records": F.count_if(F.col("corrupt_record").isNotNull()),
        "started_unparseable": F.count_if(F.col("started_at").isNotNull() & s.isNull()),
        "ended_unparseable": F.count_if(F.col("ended_at").isNotNull() & e.isNull()),
        "ended_before_started": F.count_if(dur < 0),
        "zero_duration": F.count_if(dur == 0),
        "over_24h": F.count_if(dur > 86400),
        "started_in_other_month": F.count_if(
            F.expr("date_format(try_cast(started_at as timestamp_ntz), 'yyyyMM')")
            != F.col("src_month")),
        "start_coords_outside_nyc_area": F.count_if(
            la.isNotNull() & ((la < 40.0) | (la > 41.5) | (lo < -75.0) | (lo > -73.0))),
    }
    res = df.agg(*[v.alias(k) for k, v in checks.items()]).first().asDict()
    total = res["rows"]
    print("\n== Checks ==")
    for k, v in res.items():
        print(f"{k:32} {v:>12,}  {100 * v / total:6.3f}%")

    print("\n== Null or blank values per column ==")
    nulls = df.agg(*[F.count_if(F.col(c).isNull() | (F.trim(F.col(c)) == "")).alias(c)
                     for c in data_cols]).first().asDict()
    for c, v in nulls.items():
        print(f"{c:22} {v:>12,}  {100 * v / total:6.2f}%")

    print("\n== rideable_type ==")
    df.groupBy("rideable_type").count().orderBy(F.desc("count")).show(truncate=False)
    print("== member_casual ==")
    df.groupBy("member_casual").count().orderBy(F.desc("count")).show(truncate=False)
    print("== started_at string length ==")
    df.groupBy(F.length("started_at").alias("len")).count().show()
    df.select("started_at", "ended_at", "start_station_id").limit(3).show(truncate=False)

    print("== Duplicates ==")
    in_month = df.select("src_month", "ride_id").distinct().count()
    global_ = df.select("ride_id").distinct().count()
    print(f"extra rows sharing a (month, ride_id): {total - in_month:,}")
    print(f"extra rows sharing a ride_id across the year: {total - global_:,}")
    spark.stop()


if __name__ == "__main__":
    main()