import os

os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd

from lakehouse.spark_session import get_spark


def validate(source, df, label):
    asset = source.add_dataframe_asset("trips")
    batch_def = asset.add_batch_definition_whole_dataframe("whole")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    not_null = batch.validate(gxe.ExpectColumnValuesToNotBeNull(column="ride_id"))
    in_range = batch.validate(
        gxe.ExpectColumnValuesToBeBetween(column="minutes", min_value=0, max_value=1440))
    print(f"[{label}] ride_id not null: success={not_null.success} "
          f"unexpected={not_null.result.get('unexpected_count')}")
    print(f"[{label}] minutes in range: success={in_range.success} "
          f"unexpected={in_range.result.get('unexpected_count')}")


def main():
    print("great_expectations version:", gx.__version__)

    try:
        context = gx.get_context(mode="ephemeral")
        df = pd.DataFrame({"ride_id": ["a", "b", None], "minutes": [5, 12, -3]})
        validate(context.data_sources.add_pandas("smoke_pandas"), df, "pandas")
        print("GX + pandas: OK")
    except Exception as exc:
        print("GX + pandas: FAILED:", type(exc).__name__, str(exc)[:300])

    try:
        spark = get_spark("smoke_gx", driver_memory="1g")
        sdf = spark.createDataFrame([("a", 5), ("b", 12), (None, -3)],
                                    "ride_id string, minutes int")
        context = gx.get_context(mode="ephemeral")
        validate(context.data_sources.add_spark("smoke_spark"), sdf, "spark")
        print("GX + Spark: OK")
        spark.stop()
    except Exception as exc:
        print("GX + Spark: FAILED:", type(exc).__name__, str(exc)[:300])


if __name__ == "__main__":
    main()