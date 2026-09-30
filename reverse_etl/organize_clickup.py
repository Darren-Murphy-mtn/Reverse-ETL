"""File stale-issue tasks into a folder, one list per repo, with a small tag vocabulary.

GitHub labels are normalized. A bug and a feature do not stay as forty raw label names.
Priority comes from that vocabulary, not from days stale: this extract can only prove
"at least 30 days" for almost every row, so staleness does not rank them.

Custom Fields are not used. The free plan allows 60 Custom Field writes for the life of
the workspace.
"""
from __future__ import annotations

import argparse
import json
import logging
from collections import Counter
from pathlib import Path
from urllib.parse import quote

import duckdb

from pipeline_common.clickup_client import ClickUpClient, ClickUpError
from pipeline_common.config import env_token, load_config
from pipeline_common.log import setup_logging

log = logging.getLogger("reverse_etl.organize")

FOLDER_NAME = "Stale issues"
REPO_LISTS = {
    "vercel/next.js": "next.js",
    "dbt-labs/dbt-core": "dbt-core",
}
LABEL_TO_TAG = {
    "bug": "bug",
    "type:bug": "bug",
    "type:feature": "feature",
    "type:docs": "docs",
    "type:tech-debt": "tech-debt",
    "status:triage": "triage",
    "triage": "triage",
    "status:needs-repro": "needs-repro",
}
TAG_COLORS = {
    "bug": ("#ffffff", "#d33d44"),
    "feature": ("#ffffff", "#1d76db"),
    "docs": ("#ffffff", "#0075ca"),
    "tech-debt": ("#ffffff", "#5319e7"),
    "triage": ("#ffffff", "#e16b16"),
    "needs-repro": ("#000000", "#fbca04"),
}
PRIORITY_HIGH = 2
PRIORITY_NORMAL = 3
PRIORITY_LOW = 4


def tags_for(label_names: list[str]) -> set[str]:
    return {LABEL_TO_TAG[name] for name in label_names if name in LABEL_TO_TAG}


def priority_for(tags: set[str]) -> int:
    if "bug" in tags or "needs-repro" in tags:
        return PRIORITY_HIGH
    if "docs" in tags and "feature" not in tags and "tech-debt" not in tags:
        return PRIORITY_LOW
    return PRIORITY_NORMAL


def load_task_ids(log_path: Path) -> dict[str, str]:
    """Map issue URL to ClickUp task id from a sync log."""
    found: dict[str, str] = {}
    for line in log_path.read_text().splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        if rec.get("action") != "create" or rec.get("status") != "ok":
            continue
        description = (rec.get("request") or {}).get("description") or ""
        url = next((part.strip() for part in description.splitlines() if part.startswith("issue URL:")), "")
        url = url.removeprefix("issue URL:").strip()
        task_id = rec.get("task_id")
        if url and task_id:
            found[url] = task_id
    return found


def load_issues(warehouse: Path) -> list[dict]:
    query = """
        with labels as (
            select issue_id, list(distinct label_name) as label_names
            from (
                select
                    _source_repo || '#' || cast(number as varchar) as issue_id,
                    unnest(labels).name as label_name
                from raw.raw_issues
                where labels is not null
            )
            group by issue_id
        )
        select
            s.issue_id,
            s.repo_full_name,
            s.html_url,
            coalesce(l.label_names, []) as label_names
        from marts.fct_stale_issues as s
        left join labels as l on l.issue_id = s.issue_id
        order by s.repo_full_name, s.issue_number
    """
    with duckdb.connect(str(warehouse), read_only=True) as con:
        rows = con.execute(query).fetchall()
    return [
        {
            "issue_id": issue_id,
            "repo_full_name": repo,
            "html_url": url,
            "label_names": list(label_names or []),
        }
        for issue_id, repo, url, label_names in rows
    ]


def _json(client: ClickUpClient, method: str, url: str, payload: dict | None = None) -> dict:
    kwargs = {} if payload is None else {"json": payload}
    resp = client.request(method, url, **kwargs)
    raw = resp.text
    return json.loads(raw) if raw else {}


def ensure_structure(client: ClickUpClient, space_id: str) -> dict[str, str]:
    """Return repo full name -> ClickUp list id, creating the folder and lists if needed."""
    folders = _json(client, "GET", f"/space/{space_id}/folder").get("folders") or []
    folder = next((f for f in folders if f.get("name") == FOLDER_NAME and not f.get("hidden")), None)
    if folder is None:
        folder = _json(client, "POST", f"/space/{space_id}/folder", {"name": FOLDER_NAME})
        log.info("created folder %s", FOLDER_NAME)
    folder_id = folder["id"]
    existing = {lst.get("name"): lst.get("id") for lst in folder.get("lists") or []}
    if not existing:
        listed = _json(client, "GET", f"/folder/{folder_id}/list").get("lists") or []
        existing = {lst.get("name"): lst.get("id") for lst in listed}
    list_ids = {}
    for repo, name in REPO_LISTS.items():
        if name not in existing:
            created = _json(
                client,
                "POST",
                f"/folder/{folder_id}/list",
                {"name": name, "content": f"Open {repo} issues with no human activity for at least 30 days."},
            )
            existing[name] = created["id"]
            log.info("created list %s", name)
        list_ids[repo] = existing[name]
    for tag, (fg, bg) in TAG_COLORS.items():
        try:
            _json(
                client,
                "POST",
                f"/space/{space_id}/tag",
                {"tag": {"name": tag, "tag_fg": fg, "tag_bg": bg}},
            )
        except ClickUpError as e:
            if e.status not in (400, 409):
                raise
            log.info("tag %s already exists", tag)
    return list_ids


def scan_list_ids(client: ClickUpClient, primary_list_id: str) -> list[str]:
    """Primary list plus the per-repo lists, so a later sync does not recreate moved tasks."""
    found = [primary_list_id]
    listing = _json(client, "GET", f"/list/{primary_list_id}")
    space_id = (listing.get("space") or {}).get("id")
    if not space_id:
        return found
    folders = _json(client, "GET", f"/space/{space_id}/folder").get("folders") or []
    folder = next((f for f in folders if f.get("name") == FOLDER_NAME and not f.get("hidden")), None)
    if folder is None:
        return found
    lists = folder.get("lists") or _json(client, "GET", f"/folder/{folder['id']}/list").get("lists") or []
    for lst in lists:
        if lst.get("id") and lst["id"] not in found:
            found.append(lst["id"])
    return found


def _delete_tag(client: ClickUpClient, task_id: str, tag: str) -> None:
    try:
        _json(client, "DELETE", f"/task/{task_id}/tag/{quote(tag)}")
    except ClickUpError as e:
        if e.status != 404:
            raise


def _priority_name(priority: int) -> str:
    return {2: "high", 3: "normal", 4: "low"}[priority]


def organize(client: ClickUpClient, space_id: str, rows: list[dict], task_ids: dict[str, str]) -> Counter:
    list_ids = ensure_structure(client, space_id)
    counts: Counter = Counter()
    consecutive_errors = 0
    # One probe task was tagged "bug" while checking the API. Drop vocabulary tags it should not have.
    probe_id = next(iter(task_ids.values()), None)
    for row in rows:
        task_id = task_ids.get(row["html_url"])
        if not task_id:
            counts["missing_task"] += 1
            continue
        tags = tags_for(row["label_names"])
        priority = priority_for(tags)
        repo = row["repo_full_name"]
        try:
            _json(client, "PUT", f"/task/{task_id}", {"priority": priority})
            if task_id == probe_id:
                for tag in TAG_COLORS:
                    if tag not in tags:
                        _delete_tag(client, task_id, tag)
            for tag in sorted(tags):
                _json(client, "POST", f"/task/{task_id}/tag/{quote(tag)}")
            _json(
                client,
                "PUT",
                f"https://api.clickup.com/api/v3/workspaces/{client.workspace_id}/tasks/{task_id}/home_list/{list_ids[repo]}",
                {},
            )
            counts["ok"] += 1
            counts[f"{REPO_LISTS[repo]}:{_priority_name(priority)}"] += 1
            for tag in tags:
                counts[f"tag:{tag}"] += 1
            consecutive_errors = 0
            if counts["ok"] % 50 == 0:
                log.info("organized %s so far (%s errors)", counts["ok"], counts["error"])
        except ClickUpError as e:
            counts["error"] += 1
            consecutive_errors += 1
            log.error("organize failed for %s: %s", row["issue_id"], e)
            if consecutive_errors >= 5:
                log.error("stopping after %s consecutive failures", consecutive_errors)
                break
    return counts


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    cfg = load_config()
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--live", action="store_true", help="apply folder, tags, and priority in ClickUp")
    ap.add_argument("--warehouse", type=Path, default=cfg.warehouse)
    ap.add_argument("--sync-log", type=Path, help="sync jsonl to map issue URLs to task ids")
    args = ap.parse_args(argv)

    rows = load_issues(args.warehouse)
    log_path = args.sync_log or max(cfg.sync_logs.glob("sync_*.jsonl"), default=None)
    if log_path is None:
        log.error("no sync log under %s", cfg.sync_logs)
        return 2
    task_ids = load_task_ids(log_path)
    summary: Counter = Counter()
    for row in rows:
        tags = tags_for(row["label_names"])
        summary[REPO_LISTS.get(row["repo_full_name"], row["repo_full_name"])] += 1
        summary[f"priority:{_priority_name(priority_for(tags))}"] += 1
        for tag in tags:
            summary[f"tag:{tag}"] += 1
        if row["html_url"] not in task_ids:
            summary["missing_task"] += 1
    log.info("plan for %s tasks: %s", len(rows), dict(summary))
    if not args.live:
        log.info("DRY RUN, no API calls made.")
        return 0

    token = env_token("CLICKUP_API_KEY")
    list_id = env_token("CLICKUP_LIST_ID")
    if not token or not list_id:
        log.error("--live requires CLICKUP_API_KEY and CLICKUP_LIST_ID")
        return 2
    client = ClickUpClient(token)
    listing = _json(client, "GET", f"/list/{list_id}")
    space_id = (listing.get("space") or {}).get("id")
    team_id = None
    # workspace id is on the authorized team, not the list payload
    teams = _json(client, "GET", "/team").get("teams") or []
    if teams:
        team_id = teams[0].get("id")
    if not space_id or not team_id:
        log.error("could not resolve the ClickUp space or workspace")
        return 2
    client.workspace_id = team_id
    counts = organize(client, space_id, rows, task_ids)
    log.info("organize finished: %s", dict(counts))
    return 1 if counts["error"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
