from lakehouse.quality import build_expectations, row_violations

SCHEMA = ("src_month string, ride_id string, duration_seconds long, started_at timestamp_ntz, "
          "ended_at timestamp_ntz, start_lat double, start_lng double, end_lat double, "
          "end_lng double")


def violations(spark, **kw):
    base = dict(src_month="202101", ride_id="r1", duration_seconds=600, started_at=None,
                ended_at=None, start_lat=40.7, start_lng=-74.0, end_lat=40.8, end_lng=-73.9)
    base.update(kw)
    df = spark.createDataFrame([tuple(base[c.split()[0]] for c in SCHEMA.split(", "))], SCHEMA)
    return sorted(r["rule"] for r in row_violations(df).collect())


def test_normal_row_has_no_violations(spark):
    assert violations(spark) == []


def test_duration_boundary(spark):
    assert violations(spark, duration_seconds=86400) == []
    assert violations(spark, duration_seconds=86401) == ["duration_over_24h"]


def test_start_coordinates_outside_area(spark):
    assert violations(spark, start_lat=0.0, start_lng=0.0) == ["coords_outside_area"]


def test_end_coordinates_outside_area(spark):
    assert violations(spark, end_lat=51.5, end_lng=-0.1) == ["coords_outside_area"]


def test_missing_end_coordinates_are_normal(spark):
    assert violations(spark, end_lat=None, end_lng=None) == []


def test_missing_start_coordinates_flagged_once(spark):
    assert violations(spark, start_lat=None, start_lng=None) == ["start_coords_missing"]


def test_suite_covers_the_key_checks():
    names = {type(e).__name__ for e in build_expectations()}
    assert {"ExpectColumnValuesToBeUnique", "ExpectColumnValuesToNotBeNull",
            "ExpectColumnValuesToBeInSet", "ExpectTableRowCountToBeBetween"} <= names