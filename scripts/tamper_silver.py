import argparse

from lakehouse import config
from lakehouse.spark_session import get_spark


def main():
    p = argparse.ArgumentParser(description="Make one silver row stale, to test the MERGE update")
    p.add_argument("--month", required=True)
    args = p.parse_args()
    spark = get_spark("tamper_silver", driver_memory="2g")
    t = config.SILVER_TABLE
    rid = spark.sql(f"SELECT ride_id FROM {t} WHERE src_month = '{args.month}' LIMIT 1").first()[0]
    # row_hash is changed too: this simulates a row that is out of date relative to bronze
    spark.sql(f"UPDATE {t} SET member_casual = 'TAMPERED', row_hash = row_hash + 1 "
              f"WHERE src_month = '{args.month}' AND ride_id = '{rid}'")
    print(f"Tampered with ride_id={rid} in {args.month}")
    spark.stop()


if __name__ == "__main__":
    main()