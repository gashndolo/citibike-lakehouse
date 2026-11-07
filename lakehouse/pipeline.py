"""Glue between the phase modules and the orchestrator.

Heavy imports (Spark, Great Expectations) are lazy, so loading the Dagster
definitions stays fast and the orchestration tests need no Spark.
Each run_* function does one month of one layer and returns a small dict.
"""


def month_key(partition_key: str) -> str:
    """Dagster partition key '2021-03-01' -> source month '202103'."""
    return partition_key[:7].replace("-", "")


def ensure_columns(spark, table: str, schema: str) -> None:
    """Add any column named in a 'name type, name type' string that the table lacks."""
    existing = {c.lower() for c in spark.table(table).columns}
    for part in schema.split(", "):
        name, dtype = part.split(" ", 1)
        if name.lower() not in existing:
            spark.sql(f"ALTER TABLE {table} ADD COLUMN `{name}` {dtype}")


def _latest(spark, table: str, month: str, columns: str, where: str = ""):
    return spark.sql(
        f"SELECT {columns} FROM {table} WHERE src_month = '{month}' {where} "
        "ORDER BY loaded_at DESC LIMIT 1").first().asDict()


def run_bronze(spark, month: str, force: bool = False) -> dict:
    from lakehouse import bronze, config, landing

    zpath = landing.download_zip(int(month[:4]))
    members = [m for m in landing.list_members(zpath) if m.month == month]
    if not members:
        raise ValueError(f"{month} not found in {zpath.name}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.BRONZE_NS}")
    spark.sql(bronze.LOAD_LOG_DDL)
    fingerprint = landing.month_fingerprint(members)
    if not force and bronze.last_fingerprint(spark, month) == fingerprint:
        return {"status": "skipped", "reason": "source unchanged"}
    bronze.load_month(spark, zpath, month, members, fingerprint)
    row = _latest(spark, config.LOAD_LOG_TABLE, month,
                  "rows_loaded, rows_replaced, corrupt_rows, ROUND(seconds, 1) AS seconds",
                  "AND status = 'success'")
    return {"status": "loaded", **row}


def run_silver(spark, month: str, force: bool = False) -> dict:
    from lakehouse import bronze, config, silver

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.SILVER_NS}")
    for ddl in (silver.SILVER_DDL, silver.REJECTED_DDL, silver.LOG_DDL):
        spark.sql(ddl)
    ensure_columns(spark, config.SILVER_LOG_TABLE, silver.LOG_SCHEMA)
    fingerprint = bronze.last_fingerprint(spark, month)
    if fingerprint is None:
        raise ValueError(f"{month} is not loaded into bronze yet")
    if not force and silver.last_bronze_fingerprint_loaded(spark, month) == fingerprint:
        return {"status": "skipped", "reason": "bronze unchanged"}
    silver.load_month(spark, month, fingerprint)
    row = _latest(spark, config.SILVER_LOG_TABLE, month,
                  "rows_in, rows_rejected, duplicates_removed, rows_silver, inserted, updated, "
                  "ROUND(seconds, 1) AS seconds", "AND status = 'success'")
    return {"status": "loaded", **row}


def run_quality(spark, month: str, force: bool = False) -> dict:
    from lakehouse import config, quality

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.QUALITY_NS}")
    for ddl in (quality.QUARANTINE_DDL, quality.RESULTS_DDL, quality.STATUS_DDL):
        spark.sql(ddl)
    silver_loaded_at = quality.latest_silver_load(spark, month)
    if silver_loaded_at is None:
        raise ValueError(f"{month} is not loaded into silver yet")
    checked_now = force or quality.last_checked_silver_load(spark, month) != silver_loaded_at
    if checked_now:
        quality.run_gate(spark, month, silver_loaded_at)
    row = spark.sql(
        "SELECT status, rows_checked, quarantined_rows, failed_expectations "
        f"FROM {config.GATE_STATUS_TABLE} WHERE src_month = '{month}'").first().asDict()
    return {"checked_now": checked_now, **row}


def run_gold(spark, month: str, force: bool = False) -> dict:
    from lakehouse import config, gold

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.GOLD_NS}")
    for ddl in (gold.DAILY_DDL, gold.STATION_DDL, gold.LOG_DDL):
        spark.sql(ddl)
    silver_loaded_at = gold.gate_decision(spark, month)
    if silver_loaded_at is None:
        raise RuntimeError(f"{month} is blocked by the quality gate")
    if not force and gold.last_built(spark, month) == silver_loaded_at:
        return {"status": "skipped", "reason": "silver unchanged"}
    gold.load_month(spark, month, silver_loaded_at)
    row = _latest(spark, config.GOLD_LOG_TABLE, month,
                  "rides_in, daily_rows, station_rows, ROUND(seconds, 1) AS seconds",
                  "AND status = 'success'")
    return {"status": "loaded", **row}