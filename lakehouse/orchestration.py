"""Dagster definitions: one asset per layer, partitioned by month.

Run the UI:      scripts/dagster_dev.sh
One month, CLI:  dagster asset materialize -m lakehouse.orchestration \
                     --select '*' --partition 2021-03-01
"""
import importlib.util

import dagster as dg

from lakehouse import pipeline

MONTHS = dg.MonthlyPartitionsDefinition(start_date="2021-01-01", end_date="2022-01-01")
GROUP = "citibike_lakehouse"


@dg.resource
def spark_resource(_context):
    """One Spark session per run, shared by every asset in it, stopped afterwards."""
    from lakehouse.spark_session import get_spark

    spark = get_spark("dagster_pipeline")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    try:
        yield spark
    finally:
        spark.stop()


def _metadata(result: dict) -> dict:
    return {k: v for k, v in result.items() if v is not None}


@dg.asset(partitions_def=MONTHS, group_name=GROUP, required_resource_keys={"spark"},
          description="Raw monthly CSVs from the Citi Bike yearly zip, loaded as text into "
                      "Iceberg. Skipped when the source file is unchanged.")
def bronze_trips(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    month = pipeline.month_key(context.partition_key)
    return dg.MaterializeResult(
        metadata=_metadata(pipeline.run_bronze(context.resources.spark, month)))


@dg.asset(partitions_def=MONTHS, group_name=GROUP, deps=[bronze_trips],
          required_resource_keys={"spark"},
          description="Typed, deduplicated trips (MERGE upsert) plus a rejected-rows table.")
def silver_trips(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    month = pipeline.month_key(context.partition_key)
    return dg.MaterializeResult(
        metadata=_metadata(pipeline.run_silver(context.resources.spark, month)))


@dg.asset(partitions_def=MONTHS, group_name=GROUP, deps=[silver_trips],
          required_resource_keys={"spark"},
          description="Quality gate: row-level quarantine and Great Expectations batch checks. "
                      "Fails the run, so nothing downstream is built, if the month is unhealthy.")
def quality_gate(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    month = pipeline.month_key(context.partition_key)
    result = pipeline.run_quality(context.resources.spark, month)
    if result["status"] != "pass":
        raise dg.Failure(
            description=f"Quality gate failed for {month}: {result.get('failed_expectations')}",
            metadata=_metadata(result))
    return dg.MaterializeResult(metadata=_metadata(result))


assets = [bronze_trips, silver_trips, quality_gate]

# The gold layer is optional: it appears here once lakehouse/gold.py exists.
if importlib.util.find_spec("lakehouse.gold") is not None:

    @dg.asset(partitions_def=MONTHS, group_name=GROUP, deps=[quality_gate],
              required_resource_keys={"spark"},
              description="Daily and per-station aggregates built from trusted, "
                          "non-quarantined rides of gate-passing months only.")
    def gold_tables(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        month = pipeline.month_key(context.partition_key)
        return dg.MaterializeResult(
            metadata=_metadata(pipeline.run_gold(context.resources.spark, month)))

    assets.append(gold_tables)

monthly_pipeline = dg.define_asset_job(
    "monthly_pipeline", selection=dg.AssetSelection.all(),
    description="Bronze, silver, quality gate and gold for one month.")

defs = dg.Definitions(
    assets=assets,
    jobs=[monthly_pipeline],
    resources={"spark": spark_resource},
    executor=dg.in_process_executor,     # one process, so one shared Spark session
)