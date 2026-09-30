"""GitHub omits optional keys instead of nulling them, so the Parquet schema varies by extraction.

`pull_request` appears only on pull requests and `label` only on labeled/unlabeled events, so an
extraction containing neither (a sandbox-only run, a narrow window, a --max-pages sample) lands
Parquet without those columns. The staging models must still build.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from extract import to_parquet
from load.load_to_duckdb import load
from pipeline_common.config import ROOT

REPO = "example-org/plain"
SLUG = "example-org__plain"
RUN_ID = "20260101T000000Z"


def _issue(number: int) -> dict:
    """An ordinary issue: no `pull_request` key, exactly as the API returns for non-PRs."""
    api = f"https://api.github.com/repos/{REPO}"
    return {
        "id": 900 + number, "number": number, "title": f"Issue {number}",
        "url": f"{api}/issues/{number}", "repository_url": api,
        "html_url": f"https://github.com/{REPO}/issues/{number}",
        "user": {"login": "dev_01", "type": "User"}, "state": "open", "state_reason": None,
        "comments": 0, "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z",
        "closed_at": None, "body": "b",
    }


def _event(number: int) -> dict:
    """A `closed` event: carries no `label` key, unlike labeled/unlabeled events."""
    return {
        "id": 800 + number, "event": "closed", "actor": {"login": "dev_01", "type": "User"},
        "created_at": "2026-01-02T00:00:00Z",
        "issue": {"id": 900 + number, "number": number, "title": f"Issue {number}",
                  "repository_url": f"https://api.github.com/repos/{REPO}",
                  "html_url": f"https://github.com/{REPO}/issues/{number}", "state": "open"},
    }


def _comment(number: int) -> dict:
    api = f"https://api.github.com/repos/{REPO}"
    return {"id": 700 + number, "issue_url": f"{api}/issues/{number}",
            "user": {"login": "dev_02", "type": "User"}, "body": "hi",
            "created_at": "2026-01-03T00:00:00Z", "updated_at": "2026-01-03T00:00:00Z"}


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory) -> Path:
    """Land a run with no PRs and no label events, then load it into DuckDB."""
    base = tmp_path_factory.mktemp("optional_cols")
    repo_dir = base / "json" / "runs" / RUN_ID / SLUG
    for entity, rows in [("issues", [_issue(1), _issue(2)]),
                         ("comments", [_comment(1)]),
                         ("events", [_event(1)])]:
        (repo_dir / entity).mkdir(parents=True, exist_ok=True)
        (repo_dir / entity / "page_windowed_0001.json").write_text(json.dumps(rows))
    (repo_dir / "manifest.json").write_text(json.dumps({
        "repo_full_name": REPO, "run_id": RUN_ID, "window_start": "2025-07-05T00:00:00Z",
        "extracted_at": "2026-01-04T00:00:00Z", "truncated": False, "pages": {}, "records": {}}))

    parquet = base / "parquet"
    to_parquet.build(base / "json", parquet)
    db = base / "w.duckdb"
    load(db, parquet)
    return db


def test_parquet_really_lacks_the_optional_columns(warehouse):
    """Guards the premise: if these columns appeared, the test below would prove nothing."""
    with duckdb.connect(str(warehouse), read_only=True) as con:
        issue_cols = {c[1] for c in con.execute("pragma table_info('raw.raw_issues')").fetchall()}
        event_cols = {c[1] for c in con.execute("pragma table_info('raw.raw_events')").fetchall()}
    assert "pull_request" not in issue_cols
    assert "label" not in event_cols


def test_dbt_build_succeeds_without_the_optional_columns(warehouse):
    dbt = Path(sys.executable).with_name("dbt")
    entrypoint = [str(dbt)] if dbt.exists() else [sys.executable, "-m", "dbt.cli.main"]
    result = subprocess.run(
        [*entrypoint, "--no-use-colors", "build",
         "--project-dir", "dbt_project", "--profiles-dir", "dbt_project",
         "--target-path", str(warehouse.parent / "target")],
        cwd=ROOT, env={**os.environ, "DUCKDB_PATH": str(warehouse)},
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout[-4000:]
    assert "Binder Error" not in result.stdout

    with duckdb.connect(str(warehouse), read_only=True) as con:
        assert con.execute("select count(*) from staging.stg_issues where is_pull_request").fetchone()[0] == 0
        assert con.execute("select count(*) from staging.stg_events where label_name is not null").fetchone()[0] == 0
