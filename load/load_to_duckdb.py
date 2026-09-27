"""Load raw Parquet into DuckDB, untransformed: one raw table per source file."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb

from pipeline_common.config import load_config
from pipeline_common.log import setup_logging

log = logging.getLogger("load")
TABLES = {
    "raw_issues": "issues.parquet",
    "raw_comments": "comments.parquet",
    "raw_events": "events.parquet",
    "raw_extraction_runs": "extraction_runs.parquet",
}


def load(warehouse: Path, raw_parquet: Path) -> None:
    missing = [f for f in TABLES.values() if not (raw_parquet / f).exists()]
    if missing:
        raise SystemExit(f"missing Parquet files in {raw_parquet}: {missing}")
    with duckdb.connect(str(warehouse)) as con:
        con.execute("create schema if not exists raw")
        for table, filename in TABLES.items():
            con.execute(
                f"create or replace table raw.{table} as "
                f"select *, current_timestamp as _loaded_at from read_parquet('{raw_parquet / filename}')"
            )
            n = con.execute(f"select count(*) from raw.{table}").fetchone()[0]
            log.info("raw.%-20s %7d rows", table, n)


def main() -> None:
    setup_logging()
    cfg = load_config()
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--raw-parquet", type=Path, default=cfg.raw_parquet)
    ap.add_argument("--warehouse", type=Path, default=cfg.warehouse)
    args = ap.parse_args()
    load(args.warehouse, args.raw_parquet)


if __name__ == "__main__":
    main()
