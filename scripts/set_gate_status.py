import argparse

from lakehouse import config
from lakehouse.spark_session import get_spark


def main():
    p = argparse.ArgumentParser(description="Set a month's gate status (for the block demo)")
    p.add_argument("--month", required=True)
    p.add_argument("--status", choices=["pass", "fail"], required=True)
    args = p.parse_args()
    spark = get_spark("set_gate_status", driver_memory="1g")
    spark.sql(f"UPDATE {config.GATE_STATUS_TABLE} SET status = '{args.status}' "
              f"WHERE src_month = '{args.month}'")
    spark.sql(f"SELECT src_month, status FROM {config.GATE_STATUS_TABLE} "
              f"WHERE src_month = '{args.month}'").show()
    spark.stop()


if __name__ == "__main__":
    main()