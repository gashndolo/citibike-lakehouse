from lakehouse import config
from lakehouse.spark_session import get_spark

def main():
    spark = get_spark("show_quality", driver_memory="2g")
    print("\n== Gate status per month ==")
    spark.sql(f"""SELECT src_month, status, rows_checked, quarantined_rows,
                  failed_expectations, ROUND(seconds, 1) AS seconds
                  FROM {config.GATE_STATUS_TABLE} ORDER BY 1""").show(60, truncate=False)

    print("== Quarantine by rule ==")
    spark.sql(f"""SELECT rule, COUNT(*) AS rows, COUNT(DISTINCT ride_id) AS rides
                  FROM {config.QUARANTINE_TABLE} GROUP BY 1 ORDER BY 2 DESC""").show(truncate=False)
    print("== Distinct quarantined rides ==")
    spark.sql(f"SELECT COUNT(DISTINCT ride_id) AS rides FROM {config.QUARANTINE_TABLE}").show()

    print("== Worst month vs allowed, per check ==")
    spark.sql(f"""SELECT expectation, column_name,
                  ROUND((1 - COALESCE(mostly, 1)) * 100, 3) AS allowed_pct,
                  ROUND(MAX(unexpected_percent), 4) AS worst_pct,
                  SUM(CASE WHEN success THEN 0 ELSE 1 END) AS months_failed
                  FROM {config.GATE_RESULTS_TABLE}
                  GROUP BY expectation, column_name, mostly
                  ORDER BY expectation, column_name""").show(60, truncate=False)
    spark.stop()

if __name__ == "__main__":
    main()