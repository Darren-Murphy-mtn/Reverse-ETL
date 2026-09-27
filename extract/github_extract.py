"""Extract issues, comments and issue events for each configured repo.

Every API page is written to disk as raw JSON before anything parses it. Each run gets
its own directory, so earlier extractions are never overwritten:

    data/raw/json/runs/<run_id>/<owner>__<repo>/<entity>/page_<stream>_<n>.json
    data/raw/json/runs/<run_id>/<owner>__<repo>/manifest.json   (written last = run succeeded)
"""
from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from extract import to_parquet
from pipeline_common.config import env_token, load_config
from pipeline_common.github_client import GitHubClient
from pipeline_common.log import setup_logging

log = logging.getLogger("extract")
PER_PAGE = 100


def repo_slug(repo: str) -> str:
    owner, name = repo.split("/", 1)
    return f"{owner}__{name}"


def iso(ts: datetime) -> str:
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


@dataclass
class Stream:
    entity: str
    name: str
    path: str
    params: dict
    stop_before: datetime | None = None  # stop paging once a page reaches records older than this


@dataclass
class RepoResult:
    repo: str
    pages: dict[str, int] = field(default_factory=dict)
    records: dict[str, int] = field(default_factory=dict)
    truncated: bool = False


def streams_for(repo: str, window_start: datetime) -> list[Stream]:
    since = iso(window_start)
    base = f"/repos/{repo}/issues"
    return [
        # Everything touched inside the window (open or closed; PRs included).
        Stream("issues", "windowed", base,
               {"state": "all", "since": since, "sort": "updated", "direction": "desc", "per_page": PER_PAGE}),
        # Every open issue regardless of age: the stalest issues are exactly the ones the
        # windowed pull misses, and fct_stale_issues needs them.
        Stream("issues", "open", base,
               {"state": "open", "sort": "created", "direction": "asc", "per_page": PER_PAGE}),
        Stream("comments", "windowed", f"{base}/comments",
               {"since": since, "sort": "updated", "direction": "desc", "per_page": PER_PAGE}),
        # /issues/events has no `since` filter and returns newest first, so stop once past the window.
        Stream("events", "windowed", f"{base}/events", {"per_page": PER_PAGE}, stop_before=window_start),
    ]


def run_stream(client: GitHubClient, stream: Stream, out_dir: Path, max_pages: int | None) -> tuple[int, int, bool]:
    out_dir.mkdir(parents=True, exist_ok=True)
    pages = records = 0
    for page in client.paginate(stream.path, stream.params):
        pages += 1
        records += len(page)
        (out_dir / f"page_{stream.name}_{pages:04d}.json").write_text(json.dumps(page))
        if stream.stop_before and page and min(parse_ts(r["created_at"]) for r in page) < stream.stop_before:
            break
        if max_pages and pages >= max_pages:
            return pages, records, True
    return pages, records, False


def extract_repo(
    client: GitHubClient, repo: str, run_dir: Path, window_start: datetime, max_pages: int | None = None
) -> RepoResult:
    extracted_at = datetime.now(UTC)
    repo_dir = run_dir / repo_slug(repo)
    result = RepoResult(repo)
    for stream in streams_for(repo, window_start):
        pages, records, truncated = run_stream(client, stream, repo_dir / stream.entity, max_pages)
        key = f"{stream.entity}:{stream.name}"
        result.pages[key], result.records[key] = pages, records
        result.truncated |= truncated
        log.info("%s %-18s %4d pages %6d records%s", repo, key, pages, records, " (truncated)" if truncated else "")
    manifest = {
        "repo_full_name": repo,
        "run_id": run_dir.name,
        "window_start": iso(window_start),
        "extracted_at": iso(extracted_at),
        "truncated": result.truncated,
        "pages": result.pages,
        "records": result.records,
    }
    (repo_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return result


def main(argv: list[str] | None = None) -> None:
    setup_logging()
    cfg = load_config()
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--repos", nargs="+", help="subset of repos to extract (default: all configured)")
    ap.add_argument("--window-days", type=int, default=cfg.window_days)
    ap.add_argument("--max-pages", type=int, help="cap pages per stream (sampling / smoke tests)")
    ap.add_argument("--skip-parquet", action="store_true", help="only write raw JSON")
    args = ap.parse_args(argv)

    token = env_token("GITHUB_READ_TOKEN")
    if not token:
        log.warning("GITHUB_READ_TOKEN not set: unauthenticated limit is 60 requests/hour")
    client = GitHubClient(token=token)

    now = datetime.now(UTC)
    window_start = (now - timedelta(days=args.window_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    run_dir = cfg.raw_json / "runs" / now.strftime("%Y%m%dT%H%M%SZ")
    for repo in args.repos or cfg.extract_repos:
        extract_repo(client, repo, run_dir, window_start, args.max_pages)
    log.info("raw JSON written to %s", run_dir)

    if not args.skip_parquet:
        to_parquet.build(cfg.raw_json, cfg.raw_parquet)


if __name__ == "__main__":
    main()
