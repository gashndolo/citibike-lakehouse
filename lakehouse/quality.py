import os

os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")

import argparse  # noqa: E402
import logging  # noqa: E402
import time  # noqa: E402
from datetime import datetime, timezone  # noqa: E402

import great_expectations as gx  # noqa: E402
import great_expectations.expectations as gxe  # noqa: E402
from great_expectations.checkpoint import UpdateDataDocsAction  # noqa: E402
from pyspark import StorageLevel  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from lakehouse import config  # noqa: E402
from lakehouse.spark_session import get_spark  # noqa: E402

log = logging.getLogger("quality")

SUITE = "silver_trips"
LAT_RANGE = (40.0, 41.5)
LNG_RANGE = (-75.0, -73.0)
MAX_DURATION_S = 86400

QUARANTINE_DDL = f"""
CREATE TABLE IF NOT EXISTS {config.QUARANTINE_TABLE} (
    src_month STRING, ride_id STRING, rule STRING, duration_seconds BIGINT,
    started_at TIMESTAMP_NTZ, ended_at TIMESTAMP_NTZ
) USING iceberg PARTITIONED BY (src_month)
"""
RESULTS_DDL = f"""
CREATE TABLE IF NOT EXISTS {config.GATE_RESULTS_TABLE} (
    src_month STRING, expectation STRING, column_name STRING, mostly DOUBLE,
    success BOOLEAN, element_count BIGINT, unexpected_count BIGINT,
    unexpected_percent DOUBLE, observed_value STRING, checked_at TIMESTAMP
) USING iceberg PARTITIONED BY (src_month)
"""
STATUS_DDL = f"""
CREATE TABLE IF NOT EXISTS {config.GATE_STATUS_TABLE} (
    src_month STRING, status STRING, failed_expectations STRING, rows_checked BIGINT,
    quarantined_rows BIGINT, silver_loaded_at TIMESTAMP, seconds DOUBLE,
    checked_at TIMESTAMP
) USING iceberg PARTITIONED BY (src_month)
"""
RESULTS_SCHEMA = ("src_month string, expectation string, column_name string, mostly double, "
                  "success boolean, element_count bigint, unexpected_count bigint, "
                  "unexpected_percent double, observed_value string, checked_at timestamp")
STATUS_SCHEMA = ("src_month string, status string, failed_expectations string, "
                 "rows_checked bigint, quarantined_rows bigint, silver_loaded_at timestamp, "
                 "seconds double, checked_at timestamp")


# ---------- row-level rules ----------

def _outside_area(lat, lng):
    return (lat.isNotNull() & lng.isNotNull()
            & ((lat < LAT_RANGE[0]) | (lat > LAT_RANGE[1])
               | (lng < LNG_RANGE[0]) | (lng > LNG_RANGE[1])))


def row_rules() -> dict:
    c = F.col
    return {
        "duration_over_24h": c("duration_seconds") > MAX_DURATION_S,
        "coords_outside_area": (_outside_area(c("start_lat"), c("start_lng"))
                                | _outside_area(c("end_lat"), c("end_lng"))),
        "start_coords_missing": c("start_lat").isNull() | c("start_lng").isNull(),
    }


def row_violations(df):
    """One row per (ride, rule) for every row that breaks a row-level rule."""
    parts = [df.where(cond).select("src_month", "ride_id", F.lit(name).alias("rule"),
                                   "duration_seconds", "started_at", "ended_at")
             for name, cond in row_rules().items()]
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out


# ---------- batch-level expectations ----------

def build_expectations():
    e = gxe
    return [
        e.ExpectTableRowCountToBeBetween(min_value=100_000, max_value=10_000_000),
        *[e.ExpectColumnValuesToNotBeNull(column=c) for c in (
            "ride_id", "started_at", "ended_at", "duration_seconds",
            "rideable_type", "member_casual")],
        e.ExpectColumnValuesToBeUnique(column="ride_id"),
        e.ExpectColumnValuesToBeInSet(column="rideable_type",
                                      value_set=["classic_bike", "electric_bike", "docked_bike"]),
        e.ExpectColumnValuesToBeInSet(column="member_casual", value_set=["member", "casual"]),
        e.ExpectColumnValuesToBeBetween(column="duration_seconds", min_value=1,
                                        max_value=MAX_DURATION_S, mostly=0.995),
        e.ExpectColumnValuesToBeBetween(column="start_lat", min_value=LAT_RANGE[0],
                                        max_value=LAT_RANGE[1], mostly=0.999),
        e.ExpectColumnValuesToBeBetween(column="start_lng", min_value=LNG_RANGE[0],
                                        max_value=LNG_RANGE[1], mostly=0.999),
        e.ExpectColumnValuesToNotBeNull(column="start_station_id", mostly=0.999),
        e.ExpectColumnValuesToNotBeNull(column="end_station_id", mostly=0.98),
        e.ExpectColumnValuesToNotBeNull(column="end_lat", mostly=0.99),
    ]


def _get_or_add(getter, adder):
    try:
        return getter()
    except Exception:
        return adder()


def _checkpoint():
    context = gx.get_context(mode="file", project_root_dir=str(config.PROJECT_ROOT))
    source = context.data_sources.add_or_update_spark("spark")
    asset = _get_or_add(lambda: source.get_asset("silver_month"),
                        lambda: source.add_dataframe_asset("silver_month"))
    batch_def = _get_or_add(lambda: asset.get_batch_definition("whole"),
                            lambda: asset.add_batch_definition_whole_dataframe("whole"))
    suite = context.suites.add_or_update(
        gx.ExpectationSuite(name=SUITE, expectations=build_expectations()))
    vd = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name=f"{SUITE}_vd", data=batch_def, suite=suite))
    return context.checkpoints.add_or_update(gx.Checkpoint(
        name=f"{SUITE}_cp", validation_definitions=[vd],
        actions=[UpdateDataDocsAction(name="update_data_docs")]))


def validate(df, store: bool = False):
    """Validate a DataFrame. store=True records the run in Data Docs; False is a dry run."""
    if store:
        result = _checkpoint().run(batch_parameters={"dataframe": df})
        return list(result.run_results.values())[0]
    ctx = gx.get_context(mode="ephemeral")
    asset = ctx.data_sources.add_spark("adhoc").add_dataframe_asset("batch")
    batch = (asset.add_batch_definition_whole_dataframe("whole")
             .get_batch(batch_parameters={"dataframe": df}))
    return batch.validate(gx.ExpectationSuite(name=SUITE, expectations=build_expectations()))


def parse_results(vr, month: str):
    now = datetime.now(timezone.utc)
    rows = []
    for r in vr.results:
        cfg, res = r.expectation_config, (r.result or {})
        obs = res.get("observed_value")
        rows.append((month, cfg.type, cfg.kwargs.get("column"), cfg.kwargs.get("mostly"),
                     bool(r.success), res.get("element_count"), res.get("unexpected_count"),
                     res.get("unexpected_percent"), None if obs is None else str(obs), now))
    return rows


def print_results(rows):
    print(f"{'expectation':45} {'column':18} {'mostly':>7} {'ok':>6} {'unexpected %':>13}")
    for _, exp, col, mostly, ok, _, _, pct, obs, _ in rows:
        shown = f"{pct:.4f}" if pct is not None else (obs or "")
        print(f"{exp:45} {str(col or ''):18} {str(mostly or ''):>7} {str(ok):>6} {shown:>13}")


# ---------- gate ----------

def latest_silver_load(spark, month):
    row = spark.sql(f"SELECT MAX(loaded_at) AS t FROM {config.SILVER_LOG_TABLE} "
                    f"WHERE src_month = '{month}' AND status = 'success'").first()
    return row["t"]


def last_checked_silver_load(spark, month):
    rows = spark.sql(f"SELECT silver_loaded_at FROM {config.GATE_STATUS_TABLE} "
                     f"WHERE src_month = '{month}'").collect()
    return rows[0]["silver_loaded_at"] if rows else None


def run_gate(spark, month: str, silver_loaded_at) -> str:
    t0 = time.time()
    df = (spark.table(config.SILVER_TABLE).where(F.col("src_month") == month)
          .persist(StorageLevel.MEMORY_AND_DISK))
    try:
        rows_checked = df.count()
        if rows_checked == 0:
            raise ValueError(f"{month}: no silver rows")

        row_violations(df).writeTo(config.QUARANTINE_TABLE).overwrite(
            F.col("src_month") == month)
        quarantined = (spark.table(config.QUARANTINE_TABLE)
                       .where(F.col("src_month") == month).select("ride_id").distinct().count())

        results = parse_results(validate(df, store=True), month)
    finally:
        df.unpersist()

    spark.createDataFrame(results, RESULTS_SCHEMA).writeTo(
        config.GATE_RESULTS_TABLE).overwrite(F.col("src_month") == month)
    failed = [f"{r[1]}:{r[2]}" if r[2] else r[1] for r in results if not r[4]]
    status = "fail" if failed else "pass"
    seconds = time.time() - t0
    spark.createDataFrame(
        [(month, status, ",".join(failed) or None, rows_checked, quarantined,
          silver_loaded_at, seconds, datetime.now(timezone.utc))],
        STATUS_SCHEMA).writeTo(config.GATE_STATUS_TABLE).overwrite(F.col("src_month") == month)
    log.info("%s: %s | rows=%s quarantined=%s failed=%s | %.1fs", month, status.upper(),
             f"{rows_checked:,}", f"{quarantined:,}", failed or "-", seconds)
    return status


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(description="Run the quality gate on silver months")
    p.add_argument("--months", nargs="*", help="YYYYMM values (default: all in silver)")
    p.add_argument("--force", action="store_true")
    args = p.parse_args()

    spark = get_spark("quality_gate")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    failed_months = []
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.QUALITY_NS}")
        for ddl in (QUARANTINE_DDL, RESULTS_DDL, STATUS_DDL):
            spark.sql(ddl)
        available = [r[0] for r in spark.sql(
            f"SELECT DISTINCT src_month FROM {config.SILVER_LOG_TABLE} "
            "WHERE status = 'success' ORDER BY 1").collect()]
        months = sorted(args.months) if args.months else available
        unknown = [m for m in months if m not in available]
        if unknown:
            raise SystemExit(f"Not in silver: {unknown}")
        for month in months:
            loaded_at = latest_silver_load(spark, month)
            if not args.force and last_checked_silver_load(spark, month) == loaded_at:
                log.info("%s: skip (silver unchanged since the last gate run)", month)
                continue
            if run_gate(spark, month, loaded_at) == "fail":
                failed_months.append(month)
    finally:
        spark.stop()
    if failed_months:
        log.error("Gate FAILED for: %s", failed_months)
        raise SystemExit(1)


if __name__ == "__main__":
    main()