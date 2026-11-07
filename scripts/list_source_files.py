import argparse
import xml.etree.ElementTree as ET

import requests

BUCKET_URL = "https://s3.amazonaws.com/tripdata/"
NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}


def list_objects(prefix: str):
    marker = ""
    while True:
        r = requests.get(BUCKET_URL, params={"prefix": prefix, "marker": marker}, timeout=60)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        items = root.findall("s3:Contents", NS)
        for c in items:
            yield (c.findtext("s3:Key", namespaces=NS),
                   int(c.findtext("s3:Size", namespaces=NS)),
                   c.findtext("s3:LastModified", namespaces=NS))
        if root.findtext("s3:IsTruncated", namespaces=NS) != "true" or not items:
            break
        marker = items[-1].findtext("s3:Key", namespaces=NS)


def main():
    p = argparse.ArgumentParser(description="List Citi Bike source files")
    p.add_argument("--prefix", nargs="+", default=["2020", "2021"],
                   help="key prefixes, e.g. 2020 2021 2022")
    args = p.parse_args()

    total = 0
    print(f"{'key':45} {'size_mb':>9}  last_modified")
    for prefix in args.prefix:
        for key, size, modified in list_objects(prefix):
            total += size
            print(f"{key:45} {size / 1e6:9.1f}  {modified[:10]}")
    print(f"\ntotal: {total / 1e9:.2f} GB (compressed)")


if __name__ == "__main__":
    main()