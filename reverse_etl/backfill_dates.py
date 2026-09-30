"""Set ClickUp start date to the GitHub created date, and due date to the stale-by date.

Days stale is not written. A stored number would freeze on the day of the sync.
ClickUp has no native counter that increments it, and a custom field on every task
would spend the free plan's 60 lifetime custom-field uses.
"""
from __future__ import annotations

import argparse
import logging
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from pipeline_common.clickup_client import ClickUpClient, ClickUpError
from pipeline_common.config import env_token, load_config
from pipeline_common.log import setup_logging
from reverse_etl.organize_clickup import load_task_ids
from reverse_etl.sync_stale_issues import fetch_stale_issues, render_description, stale_by

log = logging.getLogger("reverse_etl.dates")


def date_ms(value: datetime) -> int:
    """Noon UTC, so a date-only ClickUp field still shows this calendar day in US timezones."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    day = value.astimezone(UTC).replace(hour=12, minute=0, second=0, microsecond=0)
    return int(day.timestamp() * 1000)


def backfill(client: ClickUpClient, issues, task_ids: dict[str, str]) -> Counter:
    counts: Counter = Counter()
    consecutive_errors = 0
    for issue in issues:
        task_id = task_ids.get(issue.html_url)
        became_stale = stale_by(issue)
        if task_id is None or issue.created_at is None or became_stale is None:
            counts["skipped"] += 1
            continue
        payload = {
            "start_date": date_ms(issue.created_at),
            "start_date_time": False,
            "due_date": date_ms(became_stale),
            "due_date_time": False,
            "description": render_description(issue),
        }
        try:
            client.request("PUT", f"/task/{task_id}", json=payload)
            counts["ok"] += 1
            consecutive_errors = 0
            if counts["ok"] % 50 == 0:
                log.info("dated %s so far (%s errors)", counts["ok"], counts["error"])
        except ClickUpError as e:
            counts["error"] += 1
            consecutive_errors += 1
            log.error("date update failed for %s: %s", issue.issue_id, e)
            if consecutive_errors >= 5:
                log.error("stopping after %s consecutive failures", consecutive_errors)
                break
    return counts


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    cfg = load_config()
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--warehouse", type=Path, default=cfg.warehouse)
    ap.add_argument("--sync-log", type=Path)
    args = ap.parse_args(argv)

    issues = fetch_stale_issues(args.warehouse)
    log_path = args.sync_log or max(cfg.sync_logs.glob("sync_*.jsonl"), default=None)
    if log_path is None:
        log.error("no sync log under %s", cfg.sync_logs)
        return 2
    task_ids = load_task_ids(log_path)
    missing = sum(1 for issue in issues if issue.html_url not in task_ids)
    log.info("%s issues, %s matched to tasks, %s missing", len(issues), len(issues) - missing, missing)
    if not args.live:
        log.info("DRY RUN, no API calls made.")
        return 0
    token = env_token("CLICKUP_API_KEY")
    if not token:
        log.error("--live requires CLICKUP_API_KEY")
        return 2
    counts = backfill(ClickUpClient(token), issues, task_ids)
    log.info("date backfill finished: %s", dict(counts))
    return 1 if counts["error"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
