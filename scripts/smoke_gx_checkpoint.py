import os

os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")
import great_expectations as gx  # noqa: E402
import great_expectations.expectations as gxe  # noqa: E402
from great_expectations.checkpoint import UpdateDataDocsAction  # noqa: E402
from lakehouse import config  # noqa: E402
from lakehouse.spark_session import get_spark  # noqa: E402


def get_or_add(getter, adder):
    try:
        return getter()
    except Exception:
        return adder()

def main():
    print("great_expectations version:", gx.__version__)
    spark = get_spark("smoke_gx_checkpoint", driver_memory="1g")
    df = spark.createDataFrame([("a", 5), ("b", 12), (None, -3)], "ride_id string, minutes int")

    context = gx.get_context(mode="file", project_root_dir=str(config.PROJECT_ROOT))
    source = context.data_sources.add_or_update_spark("spark")
    asset = get_or_add(lambda: source.get_asset("smoke"),
                       lambda: source.add_dataframe_asset("smoke"))
    batch_def = get_or_add(lambda: asset.get_batch_definition("whole"),
                           lambda: asset.add_batch_definition_whole_dataframe("whole"))
    suite = context.suites.add_or_update(gx.ExpectationSuite(
        name="smoke_suite",
        expectations=[gxe.ExpectColumnValuesToNotBeNull(column="ride_id"),
                      gxe.ExpectColumnValuesToBeBetween(column="minutes", min_value=0,
                                                        max_value=1440)]))
    vd = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name="smoke_vd", data=batch_def, suite=suite))
    cp = context.checkpoints.add_or_update(gx.Checkpoint(
        name="smoke_cp", validation_definitions=[vd],
        actions=[UpdateDataDocsAction(name="update_data_docs")]))

    result = cp.run(batch_parameters={"dataframe": df})
    print("checkpoint success (expected False):", result.success)
    for vr in result.run_results.values():
        for r in vr.results:
            cfg = r.expectation_config
            print(" ", cfg.type, "| column:", cfg.kwargs.get("column"),
                  "| success:", r.success, "| unexpected:", (r.result or {}).get("unexpected_count"))

    docs = config.PROJECT_ROOT / "gx" / "uncommitted" / "data_docs" / "local_site" / "index.html"
    print("Data Docs built:", docs.exists(), docs)
    spark.stop()







if __name__ == "__main__":
    main()