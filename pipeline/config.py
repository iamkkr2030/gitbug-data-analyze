import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TRANSFORM_VERSION = "v1"
MAX_ID = 2**63 - 1
HEADERS = ["Issue id", "Duplicate id"]


def data_root():
    return Path(os.environ.get("GITBUGS_DATA_DIR", ROOT / "data")).resolve()


def source_path():
    return Path(os.environ.get("GITBUGS_SOURCE", ROOT / "gitbugs_full_data.csv")).resolve()


def mysql_config():
    return {
        "host": os.environ.get("MYSQL_HOST", "127.0.0.1"),
        "port": int(os.environ.get("MYSQL_PORT", "3306")),
        "user": os.environ.get("MYSQL_USER", "gitbugs"),
        "password": os.environ["MYSQL_PASSWORD"],
        "database": os.environ.get("MYSQL_DATABASE", "gitbugs"),
        "charset": "utf8mb4",
        "autocommit": False,
        "connect_timeout": 15,
        "read_timeout": 3600,
        "write_timeout": 3600,
    }

