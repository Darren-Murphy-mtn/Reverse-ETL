from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from pipeline_common.config import Config, ReverseEtlConfig
from pipeline_common.github_client import GitHubClient
from reverse_etl.sync_stale_issues import (
    BLOCKED,
    POST,
    SKIP_LIVE,
    SKIP_WAREHOUSE,
    SnapshotTooOld,
    StaleIssue,
    SyncLog,
    check_snapshot_freshness,
    execute_live,
    plan,
    render_comment,
)
from tests.conftest import FakeSession, make_response

SANDBOX = "me/sandbox"
MARKER = "<!-- reverse-etl:stale-flag -->"
NOW = datetime(2026, 9, 27, 12, tzinfo=UTC)


def issue(n: int, repo: str = SANDBOX, flagged: bool = False, lower_bound: bool = False,
          age_h: float = 1) -> StaleIssue:
    return StaleIssue(f"{repo}#{n}", repo, n, f"title {n}", f"https://github.com/{repo}/issues/{n}", 45,
                      lower_bound, 30, flagged, (NOW - timedelta(hours=age_h)).replace(tzinfo=None))


def cfg(tmp_path: Path) -> Config:
    return Config([], SANDBOX, 180, tmp_path, tmp_path, tmp_path / "w.duckdb", tmp_path / "logs",
                  ReverseEtlConfig(MARKER, "stale", 24, 0))


def test_plan_blocks_everything_outside_the_allowlist():
    decisions = plan([issue(1), issue(2, flagged=True), issue(3, repo="vercel/next.js")], {SANDBOX})
    assert [d for _, d in decisions] == [POST, SKIP_WAREHOUSE, BLOCKED]


def test_comment_carries_marker_and_lower_bound_wording():
    body = render_comment(issue(1, lower_bound=True), MARKER)
    assert body.startswith(MARKER)
    assert "at least 45 days" in body


def test_freshness_guard():
    check_snapshot_freshness([issue(1, age_h=2)], 24, NOW)
    with pytest.raises(SnapshotTooOld):
        check_snapshot_freshness([issue(1, age_h=30)], 24, NOW)


def live_router(flagged_numbers: set[int], fail_comment_on: set[int] = frozenset()):
    def route(method, url, kw):
        n = int(url.split("/issues/")[1].split("/")[0]) if "/issues/" in url else None
        if method == "GET" and url.endswith("/comments"):
            body = [{"body": f"{MARKER}\nold flag"}] if n in flagged_numbers else [{"body": "hi"}]
            return make_response(200, body)
        if method == "GET" and "/labels/" in url:
            return make_response(404, {"message": "Not Found"})
        if method == "POST" and url.endswith("/comments") and n in fail_comment_on:
            return make_response(422, {"message": "Validation Failed"})
        return make_response(201, {"html_url": url})
    return route


def run_live(tmp_path, decisions, router):
    session = FakeSession(router)
    client = GitHubClient(session=session, sleep=lambda s: None)
    sync_log = SyncLog(tmp_path / "logs", NOW)
    counts = execute_live(decisions, client, cfg(tmp_path), sync_log, apply_label=True, sleep=lambda s: None)
    return counts, session, sync_log


def test_live_sync_is_idempotent_against_live_state(tmp_path):
    decisions = plan([issue(1), issue(2)], {SANDBOX})
    counts, session, _ = run_live(tmp_path, decisions, live_router(flagged_numbers={2}))
    posts = [(m, u) for m, u, _ in session.calls if m == "POST"]
    assert counts[SKIP_LIVE] == 1
    assert counts["comment:ok"] == 1 and counts["label:ok"] == 1
    assert not any("/issues/2/" in u for _, u in posts)
    assert any(u.endswith(f"/repos/{SANDBOX}/labels") for _, u in posts)  # label created once when missing


def test_live_sync_never_calls_api_for_blocked_rows(tmp_path):
    decisions = plan([issue(9, repo="pytorch/pytorch")], {SANDBOX})
    counts, session, _ = run_live(tmp_path, decisions, live_router(set()))
    assert session.calls == []
    assert counts[BLOCKED] == 1


def test_failed_comment_is_logged_and_label_is_not_applied(tmp_path):
    decisions = plan([issue(1)], {SANDBOX})
    counts, session, sync_log = run_live(tmp_path, decisions, live_router(set(), fail_comment_on={1}))
    assert counts["comment:error"] == 1 and counts["label:ok"] == 0
    assert '"status": "error"' in sync_log.path.read_text()
