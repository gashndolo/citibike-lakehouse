from pyspark.sql import SparkSession

from lakehouse import config


def get_spark(app_name: str, driver_memory: str = "3g", cores: int = 4) -> SparkSession:
    config.WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    cat = config.CATALOG
    iceberg_pkg = f"org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:{config.ICEBERG_VERSION}"

    spark = (
        SparkSession.builder.appName(app_name)
        .master(f"local[{cores}]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.jars.packages", iceberg_pkg)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{cat}.type", "hadoop")
        .config(f"spark.sql.catalog.{cat}.warehouse", str(config.WAREHOUSE_DIR))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark