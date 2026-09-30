from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from pipeline_common.clickup_client import ClickUpClient
from reverse_etl.sync_stale_issues import (
    SKIP_LIVE,
    SnapshotTooOld,
    StaleIssue,
    SyncLog,
    check_snapshot_freshness,
    execute_live,
    render_description,
)
from tests.conftest import FakeSession, make_response

NOW = datetime(2026, 9, 27, 12, tzinfo=UTC)


def issue(n: int, repo: str = "vercel/next.js", lower_bound: bool = False, age_h: float = 1) -> StaleIssue:
    return StaleIssue(
        f"{repo}#{n}",
        repo,
        n,
        f"title {n}",
        f"https://github.com/{repo}/issues/{n}",
        45,
        lower_bound,
        30,
        False,
        (NOW - timedelta(hours=age_h)).replace(tzinfo=None),
    )


def test_description_has_repo_url_and_dates():
    created = datetime(2024, 1, 2, tzinfo=UTC)
    last = datetime(2026, 8, 1, tzinfo=UTC)
    sample = issue(1, lower_bound=True)
    dated = StaleIssue(
        sample.issue_id, sample.repo_full_name, sample.issue_number, sample.title, sample.html_url,
        sample.days_since_last_activity, True, sample.stale_threshold_days, False, sample.snapshot_at,
        created, last,
    )
    text = render_description(dated)
    assert "repo: vercel/next.js" in text
    assert "issue URL: https://github.com/vercel/next.js/issues/1" in text
    assert "github created: 2024-01-02" in text
    assert "stale by: 2026-08-31 (lower bound)" in text
    assert "days stale" not in text


def test_freshness_guard():
    check_snapshot_freshness([issue(1, age_h=2)], 24, NOW)
    with pytest.raises(SnapshotTooOld):
        check_snapshot_freshness([issue(1, age_h=30)], 24, NOW)


def router(existing_urls: set[str], fail_on: set[int] = frozenset()):
    def route(method, url, kw):
        if method == "GET":
            tasks = [{"description": f"issue URL: {item}\n"} for item in existing_urls]
            return make_response(200, {"tasks": tasks})
        number = int(kw["json"]["description"].split("/issues/")[1].split()[0])
        if number in fail_on:
            return make_response(400, {"err": "nope", "ECODE": "TASK_001"})
        return make_response(200, {"id": f"task-{number}", "url": f"https://app.clickup.com/t/task-{number}"})

    return route


def run_live(tmp_path: Path, issues: list[StaleIssue], route):
    session = FakeSession(route)
    client = ClickUpClient("pk_test", session=session, sleep=lambda s: None)
    sync_log = SyncLog(tmp_path / "logs", NOW)
    counts = execute_live(issues, client, "123", sync_log)
    return counts, session, sync_log


def test_live_sync_skips_urls_already_in_a_task(tmp_path):
    issues = [issue(1), issue(2)]
    counts, session, _ = run_live(tmp_path, issues, router({issues[1].html_url}))
    posts = [kw["json"]["name"] for method, _, kw in session.calls if method == "POST"]
    assert counts[SKIP_LIVE] == 1
    assert counts["create:ok"] == 1
    assert posts == ["title 1"]


def test_failed_create_is_logged_and_later_issues_still_run(tmp_path):
    issues = [issue(1), issue(2)]
    counts, _, sync_log = run_live(tmp_path, issues, router(set(), fail_on={1}))
    assert counts["create:error"] == 1
    assert counts["create:ok"] == 1
    assert '"status": "error"' in sync_log.path.read_text()
