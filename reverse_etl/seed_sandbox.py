"""Seed the sandbox repo with synthetic issues so the reverse-ETL loop has something to act on.

Dry-run by default. Idempotent: seed issues are matched by title, so re-running only creates
what's missing. Only ever touches config.sandbox_repo.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time

from pipeline_common.config import env_token, load_config
from pipeline_common.github_client import GitHubClient
from pipeline_common.log import setup_logging

log = logging.getLogger("seed")

SEED_ISSUES = [
    {"title": "[seed] Export fails on CSVs with BOM", "labels": ["bug"]},
    {"title": "[seed] Add dark mode to settings page", "labels": ["enhancement"],
     "comments": ["Would this also cover the email templates?"]},
    {"title": "[seed] Docs: clarify token scopes in README", "labels": ["documentation"]},
    {"title": "[seed] Timeout on large workspace sync", "labels": ["bug"],
     "comments": ["Seeing this on workspaces with more than 10k tasks."]},
    {"title": "[seed] Typo in onboarding modal", "labels": ["bug"], "close": True},
    {"title": "[seed] Retry button does nothing after network drop", "labels": ["bug"],
     "close": True, "reopen": True},
    {"title": "[seed] Support bulk-editing due dates", "labels": ["enhancement"]},
    {"title": "[seed] Keyboard shortcut conflicts with browser", "labels": ["bug"]},
]
BODY = "Synthetic issue created by `reverse_etl/seed_sandbox.py` for the reverse-ETL demo. Not a real bug report."


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    cfg = load_config()
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args(argv)
    repo = cfg.sandbox_repo

    if not args.live:
        for seed in SEED_ISSUES:
            extras = [f"{len(seed.get('comments', []))} comment(s)"] + \
                     [k for k in ("close", "reopen") if seed.get(k)]
            print(f"would create in {repo}: {seed['title']}  [{', '.join(extras)}]")
        log.info("DRY RUN, no API calls made. Re-run with --live to create.")
        return 0

    token = env_token("GITHUB_WRITE_TOKEN")
    if not token:
        log.error("--live requires GITHUB_WRITE_TOKEN")
        return 2
    client = GitHubClient(token=token)
    existing = {i["title"] for page in client.paginate(f"/repos/{repo}/issues", {"state": "all", "per_page": 100})
                for i in page}

    for seed in SEED_ISSUES:
        if seed["title"] in existing:
            log.info("exists, skipping: %s", seed["title"])
            continue
        issue = client.post_json(f"/repos/{repo}/issues", {"title": seed["title"], "body": BODY,
                                                           "labels": seed["labels"]})
        number = issue["number"]
        log.info("created #%d %s", number, seed["title"])
        for body in seed.get("comments", []):
            client.post_json(f"/repos/{repo}/issues/{number}/comments", {"body": body})
            time.sleep(1)
        if seed.get("close"):
            client.request("PATCH", f"/repos/{repo}/issues/{number}", json={"state": "closed"})
        if seed.get("reopen"):
            time.sleep(2)
            client.request("PATCH", f"/repos/{repo}/issues/{number}", json={"state": "open"})
        time.sleep(1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
