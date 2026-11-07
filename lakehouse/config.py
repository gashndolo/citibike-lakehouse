import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.getenv("DATA_DIR", PROJECT_ROOT / "data")).resolve()

LANDING_DIR = DATA_DIR / "landing"
WAREHOUSE_DIR = DATA_DIR / "warehouse"

# Iceberg 1.10.0 is the first release with a Spark 4.0 runtime
ICEBERG_VERSION = os.getenv("ICEBERG_VERSION", "1.10.1")
CATALOG = "lake"

LANDING_ZIPS = LANDING_DIR / "zips"
EXTRACT_DIR = LANDING_DIR / "extracted"
SOURCE_BASE_URL = "https://s3.amazonaws.com/tripdata"

BRONZE_NS = f"{CATALOG}.bronze"
BRONZE_TABLE = f"{BRONZE_NS}.trips_raw"
LOAD_LOG_TABLE = f"{BRONZE_NS}.load_log"

SILVER_NS = f"{CATALOG}.silver"
SILVER_TABLE = f"{SILVER_NS}.trips"
REJECTED_TABLE = f"{SILVER_NS}.trips_rejected"
SILVER_LOG_TABLE = f"{SILVER_NS}.load_log"

QUALITY_NS = f"{CATALOG}.quality"
QUARANTINE_TABLE = f"{QUALITY_NS}.trips_quarantine"
GATE_RESULTS_TABLE = f"{QUALITY_NS}.gate_results"
GATE_STATUS_TABLE = f"{QUALITY_NS}.gate_status"