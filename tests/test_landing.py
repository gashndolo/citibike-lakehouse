import zipfile

from lakehouse.landing import extract_month, list_members


def test_nested_zip_month(tmp_path):
    inner = tmp_path / "202107-citibike-tripdata.zip"
    with zipfile.ZipFile(inner, "w") as z:
        z.writestr("202107-citibike-tripdata_1.csv", "ride_id,started_at\na,b\n")
        z.writestr("202107-citibike-tripdata_2.csv", "ride_id,started_at\nc,d\n")
        z.writestr("__MACOSX/._junk.csv", "x")
    outer = tmp_path / "2021.zip"
    with zipfile.ZipFile(outer, "w") as z:
        z.write(inner, "2021-citibike-tripdata/202107-citibike-tripdata.zip")

    members = list_members(outer)
    assert [(m.month, m.kind) for m in members] == [("202107", "zip")]
    files = extract_month(outer, members, tmp_path / "out")
    assert [f.name for f in files] == ["202107-citibike-tripdata_1.csv",
                                       "202107-citibike-tripdata_2.csv"]