from datetime import datetime

from lakehouse.silver import classify, dedupe_latest

COLS = ["src_month", "src_file", "ingested_at", "corrupt_record", "ride_id", "rideable_type",
        "started_at", "ended_at", "start_station_name", "start_station_id",
        "end_station_name", "end_station_id", "start_lat", "start_lng", "end_lat", "end_lng",
        "member_casual"]
SCHEMA = ("src_month string, src_file string, ingested_at timestamp, corrupt_record string, "
          "ride_id string, rideable_type string, started_at string, ended_at string, "
          "start_station_name string, start_station_id string, end_station_name string, "
          "end_station_id string, start_lat string, start_lng string, end_lat string, "
          "end_lng string, member_casual string")


def row(**kw):
    base = dict(src_month="202101", src_file="f.csv", ingested_at=datetime(2026, 1, 1),
                corrupt_record=None, ride_id="r1", rideable_type="Classic_Bike",
                started_at="2021-01-01 10:00:00", ended_at="2021-01-01 10:10:00",
                start_station_name=" Broadway ", start_station_id="HB102",
                end_station_name="", end_station_id=None, start_lat="40.7",
                start_lng="-74.0", end_lat="40.8", end_lng="-73.9", member_casual="Member")
    base.update(kw)
    return base


def make(spark, *rows):
    return spark.createDataFrame([tuple(r[c] for c in COLS) for r in rows], SCHEMA)


def one(spark, **kw):
    return classify(make(spark, row(**kw))).collect()[0].asDict()


def test_good_row_is_typed_and_cleaned(spark):
    r = one(spark)
    assert r["reject_reason"] is None
    assert r["duration_seconds"] == 600
    assert r["rideable_type"] == "classic_bike"
    assert r["member_casual"] == "member"
    assert r["start_station_name"] == "Broadway"
    assert r["end_station_name"] is None            # blank becomes NULL
    assert r["start_lat"] == 40.7


def test_unparseable_timestamp_is_rejected_not_a_crash(spark):
    assert one(spark, started_at="not a date")["reject_reason"] == "bad_started_at"


def test_fractional_seconds_parse(spark):
    r = one(spark, started_at="2021-01-01 10:00:00.123")
    assert r["started_at"] is not None and r["reject_reason"] is None


def test_ended_before_started(spark):
    r = one(spark, ended_at="2021-01-01 09:00:00")
    assert r["reject_reason"] == "ended_before_started"


def test_missing_ride_id(spark):
    assert one(spark, ride_id="  ")["reject_reason"] == "missing_ride_id"


def test_bad_coordinate_becomes_null_but_row_is_kept(spark):
    r = one(spark, start_lat="abc")
    assert r["start_lat"] is None and r["reject_reason"] is None


def test_row_hash_ignores_lineage_columns(spark):
    df = make(spark, row(src_file="a.csv"), row(src_file="b.csv", ingested_at=datetime(2026, 2, 2)))
    hashes = {r["row_hash"] for r in classify(df).collect()}
    assert len(hashes) == 1


def test_dedupe_keeps_latest_ingest(spark):
    df = make(spark, row(ingested_at=datetime(2026, 1, 1), member_casual="casual"),
              row(ingested_at=datetime(2026, 3, 1), member_casual="member"))
    out = dedupe_latest(classify(df)).collect()
    assert len(out) == 1 and out[0]["member_casual"] == "member"