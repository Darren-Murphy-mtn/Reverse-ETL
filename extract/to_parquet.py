"""Convert the latest successful raw-JSON run per repo into one Parquet file per entity.

Records are not reshaped: nested objects stay nested. Two metadata columns are added,
following the usual loader convention (cf. Fivetran's _fivetran_synced):
_source_repo and _run_id, both derived from the file path.
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import duckdb

from pipeline_common.config import load_config
from pipeline_common.log import setup_logging

log = logging.getLogger("to_parquet")
ENTITIES = ("issues", "comments", "events")
PATH_META = r"/runs/([^/]+)/([^/]+)/[a-z]+/page_[^/]+\.json$"


def latest_manifests(raw_json: Path) -> list[Path]:
    latest: dict[str, Path] = {}
    for manifest in sorted((raw_json / "runs").glob("*/*/manifest.json")):
        repo = json.loads(manifest.read_text())["repo_full_name"]
        latest[repo] = manifest  # sorted by run_id, so the last one wins
    return list(latest.values())


def _non_empty(files: list[Path]) -> list[str]:
    return [str(f) for f in files if f.read_text().strip() not in ("", "[]")]


def build(raw_json: Path, raw_parquet: Path) -> None:
    manifests = latest_manifests(raw_json)
    if not manifests:
        raise SystemExit(f"no completed extraction runs under {raw_json}/runs")
    raw_parquet.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    for entity in ENTITIES:
        files = _non_empty([f for m in manifests for f in sorted((m.parent / entity).glob("page_*.json"))])
        if not files:
            raise SystemExit(f"no non-empty {entity} pages in the latest runs")
        out = raw_parquet / f"{entity}.parquet"
        con.execute(
            f"""
            copy (
                select * exclude (filename),
                    regexp_replace(regexp_extract(filename, '{PATH_META}', 2), '__', '/') as _source_repo,
                    regexp_extract(filename, '{PATH_META}', 1) as _run_id
                from read_json($files, format = 'array', union_by_name = true, filename = true,
                               sample_size = -1, map_inference_threshold = -1,
                               maximum_object_size = 268435456)
            ) to '{out}' (format parquet)
            """,
            {"files": files},
        )
        rows = con.execute(f"select count(*) from '{out}'").fetchone()[0]
        log.info("%-9s %7d rows from %d pages -> %s", entity, rows, len(files), out)

    runs_out = raw_parquet / "extraction_runs.parquet"
    con.execute(
        f"copy (select * from read_json($files, format = 'auto', union_by_name = true)) "
        f"to '{runs_out}' (format parquet)",
        {"files": [str(m) for m in manifests]},
    )
    log.info("extraction_runs: %d repos -> %s", len(manifests), runs_out)


def main() -> None:
    setup_logging()
    cfg = load_config()
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--raw-json", type=Path, default=cfg.raw_json)
    ap.add_argument("--raw-parquet", type=Path, default=cfg.raw_parquet)
    args = ap.parse_args()
    build(args.raw_json, args.raw_parquet)


if __name__ == "__main__":
    main()
