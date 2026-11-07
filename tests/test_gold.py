from datetime import datetime
from lakehouse.gold import build_daily, build_station, exclude_quarantined


SCHEMA = ("src_month string, ride_id string, rideable_type string, member_casual string, "
          "started_at timestamp_ntz, duration_seconds long, start_station_id string, "
          "start_station_name string")




def make(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)



def test_daily_aggregation_adds_up(spark):
    df = make(spark, [
        ("202101", "a", "classic_bike", "member", datetime(2021, 1, 5, 8), 100, "S1", "One"),
        ("202101", "b", "classic_bike", "member", datetime(2021, 1, 5, 9), 200, "S1", "One"),
        ("202101", "c", "classic_bike", "member", datetime(2021, 1, 6, 9), 50, "S2", "Two"),
    ])
    out = {(str(r["ride_date"]), r["rides"], r["duration_sum_seconds"])
           for r in build_daily(df).collect()}
    assert out == {("2021-01-05", 2, 300), ("2021-01-06", 1, 50)}

def test_station_table_skips_missing_station(spark):
    df = make(spark, [
        ("202101", "a", "classic_bike", "member", datetime(2021, 1, 5), 100, "S1", "One"),
        ("202101", "b", "classic_bike", "member", datetime(2021, 1, 5), 100, None, None),
    ])
    rows = build_station(df).collect()
    assert len(rows) == 1 and rows[0]["rides"] == 1

def test_quarantined_rides_are_excluded(spark):
    df = make(spark, [
        ("202101", "a", "classic_bike", "member", datetime(2021, 1, 5), 100, "S1", "One"),
        ("202101", "b", "classic_bike", "member", datetime(2021, 1, 5), 99999, "S1", "One"),
    ])
    q = spark.createDataFrame([("b",)], "ride_id string")
    assert [r["ride_id"] for r in exclude_quarantined(df, q).collect()] == ["a"]