"""Reverse ETL: create one ClickUp task per row in marts.fct_stale_issues.

Dry-run by default: prints the plan and makes no API calls. With --live it reads
CLICKUP_LIST_ID, skips any issue whose URL is already in a task description (closed
tasks included), and logs every write attempt under logs/reverse_etl/.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

from pipeline_common.clickup_client import ClickUpClient, ClickUpError
from pipeline_common.config import env_token, load_config
from pipeline_common.log import setup_logging
from reverse_etl.organize_clickup import scan_list_ids as discover_lists

log = logging.getLogger("reverse_etl")

CREATE = "create"
SKIP_LIVE = "skip:already_in_clickup"
_TEXT_FIELDS = ("description", "text_content", "markdown_description")


@dataclass(frozen=True)
class StaleIssue:
    issue_id: str
    repo_full_name: str
    issue_number: int
    title: str
    html_url: str
    days_since_last_activity: int
    last_activity_is_lower_bound: bool
    stale_threshold_days: int
    has_stale_flag: bool
    snapshot_at: datetime
    created_at: datetime | None = None
    last_activity_at: datetime | None = None


class SnapshotTooOld(RuntimeError):
    pass


def fetch_stale_issues(warehouse: Path) -> list[StaleIssue]:
    cols = [f for f in StaleIssue.__dataclass_fields__]
    with duckdb.connect(str(warehouse), read_only=True) as con:
        rows = con.execute(
            f"select {', '.join(cols)} from marts.fct_stale_issues "
            "order by repo_full_name, days_since_last_activity desc, issue_number"
        ).fetchall()
    return [StaleIssue(**dict(zip(cols, row, strict=True))) for row in rows]


def check_snapshot_freshness(issues: list[StaleIssue], max_age_hours: float, now: datetime) -> None:
    if not issues:
        return
    oldest = min(i.snapshot_at for i in issues).replace(tzinfo=UTC)
    age_h = (now - oldest).total_seconds() / 3600
    if age_h > max_age_hours:
        raise SnapshotTooOld(
            f"warehouse snapshot is {age_h:.1f}h old (limit {max_age_hours}h). Re-run extract + "
            "load + dbt build first, or pass --allow-stale-snapshot to override."
        )


def stale_by(issue: StaleIssue) -> datetime | None:
    if issue.last_activity_at is None:
        return None
    return issue.last_activity_at + timedelta(days=issue.stale_threshold_days)


def render_description(issue: StaleIssue) -> str:
    lines = [f"repo: {issue.repo_full_name}", f"issue URL: {issue.html_url}"]
    if issue.created_at is not None:
        lines.append(f"github created: {issue.created_at:%Y-%m-%d}")
    became_stale = stale_by(issue)
    if became_stale is not None:
        bound = " (lower bound)" if issue.last_activity_is_lower_bound else ""
        lines.append(f"stale by: {became_stale:%Y-%m-%d}{bound}")
    return "\n".join(lines)


def task_text(task: dict) -> str:
    return "\n".join(str(task.get(field) or "") for field in _TEXT_FIELDS)


class SyncLog:
    def __init__(self, directory: Path, now: datetime):
        directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / f"sync_{now:%Y%m%dT%H%M%SZ}.jsonl"

    def write(self, **entry) -> None:
        entry = {"ts": datetime.now(UTC).isoformat(), **entry}
        with self.path.open("a") as f:
            f.write(json.dumps(entry, default=str) + "\n")


def execute_live(
    issues: list[StaleIssue],
    client: ClickUpClient,
    list_id: str,
    sync_log: SyncLog,
    scan_list_ids: list[str] | None = None,
) -> Counter:
    # Tasks are filed into per-repo lists after create. Scan those too, or a rerun duplicates them.
    ids = scan_list_ids or [list_id]
    existing = [task_text(task) for lid in ids for page in client.iter_task_pages(lid) for task in page]
    counts: Counter = Counter()
    consecutive_errors = 0
    for issue in issues:
        description = render_description(issue)
        if any(issue.html_url in text for text in existing):
            counts[SKIP_LIVE] += 1
            sync_log.write(action="skip", reason=SKIP_LIVE, issue_id=issue.issue_id, issue_url=issue.html_url)
            continue
        payload = {"name": issue.title, "description": description}
        try:
            resp = client.create_task(list_id, issue.title, description)
            sync_log.write(
                action="create",
                issue_id=issue.issue_id,
                request=payload,
                status="ok",
                task_id=resp.get("id"),
                response_url=resp.get("url"),
            )
            counts["create:ok"] += 1
            consecutive_errors = 0
            existing.append(description)
            if counts["create:ok"] % 50 == 0:
                log.info(
                    "created %s so far (%s skipped, %s errors)",
                    counts["create:ok"],
                    counts[SKIP_LIVE],
                    counts["create:error"],
                )
        except ClickUpError as e:
            sync_log.write(
                action="create",
                issue_id=issue.issue_id,
                request=payload,
                status="error",
                http_status=e.status,
                error=str(e),
            )
            counts["create:error"] += 1
            consecutive_errors += 1
            log.error("create failed for %s: %s", issue.issue_id, e)
            if consecutive_errors >= 5:
                log.error("stopping after %s consecutive create failures", consecutive_errors)
                break
    return counts


def print_plan(issues: list[StaleIssue]) -> None:
    for issue in issues:
        days = (
            f">={issue.days_since_last_activity}"
            if issue.last_activity_is_lower_bound
            else str(issue.days_since_last_activity)
        )
        print(f"{CREATE:<28} {issue.issue_id:<52} {days:>6}d  {issue.title[:60]}")
    if issues:
        print(f"\n--- task description for {issues[0].issue_id} ---\n{render_description(issues[0])}\n")


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    cfg = load_config()
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--live", action="store_true", help="create ClickUp tasks in CLICKUP_LIST_ID")
    ap.add_argument("--limit", type=int, help="max tasks to create in this run")
    ap.add_argument("--allow-stale-snapshot", action="store_true")
    ap.add_argument("--warehouse", type=Path, default=cfg.warehouse)
    args = ap.parse_args(argv)

    issues = fetch_stale_issues(args.warehouse)
    if args.limit is not None:
        issues = issues[: args.limit]
    now = datetime.now(UTC)
    max_age = cfg.reverse_etl.max_snapshot_age_hours

    if not args.live:
        print_plan(issues)
        log.info("DRY RUN, no API calls made. %s candidate task(s).", len(issues))
        try:
            check_snapshot_freshness(issues, max_age, now)
        except SnapshotTooOld as e:
            log.warning("a live run would refuse: %s", e)
        return 0

    token = env_token("CLICKUP_API_KEY")
    list_id = env_token("CLICKUP_LIST_ID")
    if not token or not list_id:
        log.error("--live requires CLICKUP_API_KEY and CLICKUP_LIST_ID")
        return 2
    if not args.allow_stale_snapshot:
        try:
            check_snapshot_freshness(issues, max_age, now)
        except SnapshotTooOld as e:
            log.error("%s", e)
            return 3

    sync_log = SyncLog(cfg.sync_logs, now)
    client = ClickUpClient(token)
    try:
        scan_ids = discover_lists(client, list_id)
    except ClickUpError as e:
        log.warning("could not list the Stale issues folder, scanning the primary list only: %s", e)
        scan_ids = [list_id]
    counts = execute_live(issues, client, list_id, sync_log, scan_list_ids=scan_ids)
    log.info("LIVE sync finished: %s (log: %s)", dict(counts), sync_log.path)
    return 1 if counts["create:error"] else 0


if __name__ == "__main__":
    sys.exit(main())
