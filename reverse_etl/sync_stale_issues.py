"""Reverse ETL: push fct_stale_issues from the warehouse back into GitHub.

Dry-run by default: prints the plan and makes no API calls. With --live it only ever writes to
the configured sandbox repo. It re-checks each issue for the marker comment before posting
(the warehouse is a snapshot and may be behind), refuses to run on stale snapshots, and logs
every write attempt as JSON lines under logs/reverse_etl/.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from pipeline_common.config import Config, env_token, load_config
from pipeline_common.github_client import GitHubClient, GitHubError
from pipeline_common.log import setup_logging

log = logging.getLogger("reverse_etl")

POST = "post"
SKIP_WAREHOUSE = "skip:already_flagged(warehouse)"
SKIP_LIVE = "skip:already_flagged(live)"
BLOCKED = "blocked:not_in_write_allowlist"


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


def plan(issues: list[StaleIssue], allowlist: set[str]) -> list[tuple[StaleIssue, str]]:
    decisions = []
    for issue in issues:
        if issue.repo_full_name not in allowlist:
            decisions.append((issue, BLOCKED))
        elif issue.has_stale_flag:
            decisions.append((issue, SKIP_WAREHOUSE))
        else:
            decisions.append((issue, POST))
    return decisions


def render_comment(issue: StaleIssue, marker: str) -> str:
    days = f"at least {issue.days_since_last_activity}" if issue.last_activity_is_lower_bound \
        else str(issue.days_since_last_activity)
    return (
        f"{marker}\n"
        f"**Stale issue flag** (automated reverse-ETL sync)\n\n"
        f"No human activity for **{days} days** as of the warehouse snapshot taken "
        f"{issue.snapshot_at:%Y-%m-%d %H:%M} UTC. The threshold for this repo is "
        f"{issue.stale_threshold_days} days.\n\n"
        f"_Computed in `fct_stale_issues`; bot activity and this pipeline's own writes don't count._"
    )


class SyncLog:
    def __init__(self, directory: Path, now: datetime):
        directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / f"sync_{now:%Y%m%dT%H%M%SZ}.jsonl"

    def write(self, **entry) -> None:
        entry = {"ts": datetime.now(UTC).isoformat(), **entry}
        with self.path.open("a") as f:
            f.write(json.dumps(entry, default=str) + "\n")


def already_flagged_live(client: GitHubClient, issue: StaleIssue, marker: str) -> bool:
    path = f"/repos/{issue.repo_full_name}/issues/{issue.issue_number}/comments"
    return any(marker in (c.get("body") or "") for page in client.paginate(path, {"per_page": 100}) for c in page)


def ensure_label(client: GitHubClient, repo: str, label: str, sync_log: SyncLog) -> None:
    try:
        client.get_json(f"/repos/{repo}/labels/{label}")
    except GitHubError as e:
        if e.status != 404:
            raise
        payload = {"name": label, "color": "cfd3d7", "description": "Flagged by the reverse-ETL stale-issue sync"}
        resp = client.post_json(f"/repos/{repo}/labels", payload)
        sync_log.write(action="create_label", repo=repo, request=payload, status="ok", response_url=resp.get("url"))


def execute_live(
    decisions: list[tuple[StaleIssue, str]], client: GitHubClient, cfg: Config, sync_log: SyncLog,
    apply_label: bool, sleep=time.sleep,
) -> Counter:
    rcfg = cfg.reverse_etl
    counts: Counter = Counter()
    labelled_repos: set[str] = set()
    for issue, decision in decisions:
        if decision != POST:
            counts[decision] += 1
            continue
        if already_flagged_live(client, issue, rcfg.marker):
            counts[SKIP_LIVE] += 1
            sync_log.write(action="skip", reason=SKIP_LIVE, issue_id=issue.issue_id)
            continue
        base = f"/repos/{issue.repo_full_name}/issues/{issue.issue_number}"
        writes = [("comment", f"{base}/comments", {"body": render_comment(issue, rcfg.marker)})]
        if apply_label:
            if issue.repo_full_name not in labelled_repos:
                ensure_label(client, issue.repo_full_name, rcfg.stale_label, sync_log)
                labelled_repos.add(issue.repo_full_name)
            writes.append(("label", f"{base}/labels", {"labels": [rcfg.stale_label]}))
        for kind, path, payload in writes:
            try:
                resp = client.post_json(path, payload)
                ref = resp.get("html_url") if isinstance(resp, dict) else None
                sync_log.write(action=kind, issue_id=issue.issue_id, request=payload, status="ok", response_url=ref)
                counts[f"{kind}:ok"] += 1
            except GitHubError as e:
                sync_log.write(action=kind, issue_id=issue.issue_id, request=payload, status="error",
                               http_status=e.status, error=str(e))
                counts[f"{kind}:error"] += 1
                log.error("%s failed for %s: %s", kind, issue.issue_id, e)
                break  # don't label an issue whose comment failed
            sleep(rcfg.write_delay_seconds)  # stay under GitHub's content-creation secondary limits
    return counts


def print_plan(decisions: list[tuple[StaleIssue, str]], marker: str) -> None:
    for issue, decision in decisions:
        days = f">={issue.days_since_last_activity}" if issue.last_activity_is_lower_bound \
            else str(issue.days_since_last_activity)
        print(f"{decision:<34} {issue.issue_id:<52} {days:>5}d  {issue.title[:60]}")
    sample = next((i for i, d in decisions if d == POST), None)
    if sample:
        print(f"\n--- comment that would be posted on {sample.issue_id} ---\n{render_comment(sample, marker)}\n")


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    cfg = load_config()
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--live", action="store_true", help="actually write to GitHub (sandbox repo only)")
    ap.add_argument("--limit", type=int, help="max issues to write in this run")
    ap.add_argument("--no-label", action="store_true", help="post comments only, don't apply the stale label")
    ap.add_argument("--allow-stale-snapshot", action="store_true")
    ap.add_argument("--warehouse", type=Path, default=cfg.warehouse)
    args = ap.parse_args(argv)

    issues = fetch_stale_issues(args.warehouse)
    decisions = plan(issues, allowlist={cfg.sandbox_repo})
    if args.limit is not None:
        posts = [d for d in decisions if d[1] == POST][: args.limit]
        decisions = [d for d in decisions if d[1] != POST] + posts
    now = datetime.now(UTC)
    to_post = [i for i, d in decisions if d == POST]
    max_age = cfg.reverse_etl.max_snapshot_age_hours

    if not args.live:
        print_plan(decisions, cfg.reverse_etl.marker)
        summary = Counter(d for _, d in decisions)
        log.info("DRY RUN, no API calls made. %s", dict(summary))
        try:
            check_snapshot_freshness(to_post, max_age, now)
        except SnapshotTooOld as e:
            log.warning("a live run would refuse: %s", e)
        return 0

    token = env_token("GITHUB_WRITE_TOKEN")
    if not token:
        log.error("--live requires GITHUB_WRITE_TOKEN (fine-grained, Issues: read/write, sandbox repo only)")
        return 2
    if not args.allow_stale_snapshot:
        try:
            check_snapshot_freshness(to_post, max_age, now)
        except SnapshotTooOld as e:
            log.error("%s", e)
            return 3

    sync_log = SyncLog(cfg.sync_logs, now)
    counts = execute_live(decisions, GitHubClient(token=token), cfg, sync_log, apply_label=not args.no_label)
    log.info("LIVE sync finished: %s (log: %s)", dict(counts), sync_log.path)
    return 1 if any(k.endswith(":error") for k in counts) else 0


if __name__ == "__main__":
    sys.exit(main())
