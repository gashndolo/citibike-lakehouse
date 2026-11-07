import argparse
import logging
import shutil
from collections import defaultdict

from lakehouse import config, landing


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(description="Inspect a Citi Bike yearly zip, one month at a time")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--no-download", action="store_true")
    args = p.parse_args()

    zpath = landing.zip_path(args.year) if args.no_download else landing.download_zip(args.year)
    members = landing.list_members(zpath)
    by_month = defaultdict(list)
    for m in members:
        by_month[m.month].append(m)

    kinds = defaultdict(int)
    for m in members:
        kinds[m.kind] += 1
    print(f"\nOuter zip holds: {dict(kinds)}")

    labels, raw_headers = {}, {}
    months_by_label = defaultdict(set)
    total = 0
    print(f"\n{'file':52} {'month':7} {'size_mb':>9} {'cols':>5}  schema")
    for month in sorted(by_month):
        tmp = config.EXTRACT_DIR / f"inspect_{month}"
        shutil.rmtree(tmp, ignore_errors=True)
        try:
            for f in landing.extract_month(zpath, by_month[month], tmp):
                raw = landing.read_header_file(f)
                cols = tuple(landing.normalize_columns(raw))
                label = labels.setdefault(cols, chr(ord("A") + len(labels)))
                raw_headers.setdefault(label, raw)
                months_by_label[label].add(month)
                size = f.stat().st_size
                total += size
                print(f"{f.name:52} {month:7} {size / 1e6:9.1f} {len(cols):5d}  {label}")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    n_months = len(by_month)
    print(f"\n{n_months} months, {total / 1e9:.2f} GB uncompressed CSV")
    for cols, label in labels.items():
        months = sorted(months_by_label[label])
        print(f"\nSchema {label}: {len(cols)} columns, {len(months)} month(s): {months[0]} .. {months[-1]}")
        print("  months:", ", ".join(months))
        print("  normalized:", ", ".join(cols))
        print("  raw header:", raw_headers[label])


if __name__ == "__main__":
    main()