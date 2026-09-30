"""Generate a deterministic, synthetic raw-JSON fixture in the same layout the extractor writes.

Record shapes follow the GitHub REST v3 issue / issue-comment / issue-event payloads (trimmed
to the fields that matter). All users and content are fake. CI runs the full
Parquet -> DuckDB -> dbt build path on this, with no network access or tokens.
"""
from __future__ import annotations

import argparse
import json
import random
import zlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

from extract.github_extract import iso, repo_slug
from pipeline_common.config import ROOT, load_config

# Anchored to the current hour rather than a fixed date, so the sync's freshness guard
# (max_snapshot_age_hours) sees a live snapshot. A hard-coded date makes every dry run warn
# that a live run would refuse, and the warning grows daily. Contents stay deterministic:
# the seeded RNG draws the same offsets, so only the anchor moves. Use --as-of to rebuild
# an exact historical fixture.
SNAPSHOT = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
WINDOW_DAYS = 180
RUN_ID = SNAPSHOT.strftime("%Y%m%dT%H%M%SZ")
LABELS = ["bug", "enhancement", "documentation", "triage", "needs-repro"]
HUMANS = [f"dev_{i:02d}" for i in range(1, 13)]
BOTS = ["github-actions[bot]", "dependabot[bot]"]
MARKER = "<!-- reverse-etl:stale-flag -->"


class Ids:
    def __init__(self) -> None:
        self.n = 1000

    def next(self) -> int:
        self.n += 1
        return self.n


def user(login: str) -> dict:
    kind = "Bot" if login.endswith("[bot]") else "User"
    return {"login": login, "id": zlib.crc32(login.encode()) % 10**8, "type": kind, "site_admin": False}


def issue_obj(repo: str, ids: Ids, number: int, title: str, author: str, created: datetime,
              labels: list[str], is_pr: bool) -> dict:
    api = f"https://api.github.com/repos/{repo}"
    obj = {
        "id": ids.next(), "node_id": f"I_{number}", "url": f"{api}/issues/{number}",
        "repository_url": api, "html_url": f"https://github.com/{repo}/issues/{number}",
        "number": number, "title": title, "user": user(author),
        "labels": [{"id": LABELS.index(label) + 1, "name": label, "color": "ededed", "default": False}
                   for label in labels],
        "state": "open", "state_reason": None, "locked": False, "assignees": [], "comments": 0,
        "created_at": iso(created), "updated_at": iso(created), "closed_at": None,
        "author_association": "CONTRIBUTOR", "body": f"Synthetic body for #{number}.",
        "reactions": {"total_count": 0, "+1": 0, "-1": 0},
    }
    if is_pr:
        obj["pull_request"] = {"url": f"{api}/pulls/{number}", "html_url": f"https://github.com/{repo}/pull/{number}",
                               "merged_at": None}
        obj["draft"] = False
    return obj


def comment_obj(repo: str, ids: Ids, number: int, author: str, created: datetime, body: str) -> dict:
    api = f"https://api.github.com/repos/{repo}"
    cid = ids.next()
    return {"id": cid, "url": f"{api}/issues/comments/{cid}", "issue_url": f"{api}/issues/{number}",
            "user": user(author), "created_at": iso(created), "updated_at": iso(created),
            "author_association": "CONTRIBUTOR", "body": body,
            "reactions": {"total_count": 0, "+1": 0, "-1": 0}}


def event_obj(ids: Ids, issue: dict, event: str, actor: str, created: datetime, label: str | None = None) -> dict:
    eid = ids.next()
    obj = {"id": eid, "node_id": f"E_{eid}", "url": f"{issue['repository_url']}/issues/events/{eid}",
           "actor": user(actor), "event": event, "commit_id": None, "commit_url": None,
           "created_at": iso(created),
           "issue": {k: issue[k] for k in ("id", "number", "title", "repository_url", "html_url", "state")}}
    if label:
        obj["label"] = {"name": label, "color": "ededed"}
    return obj


def touch(issue: dict, ts: datetime) -> None:
    issue["updated_at"] = max(issue["updated_at"], iso(ts))


def public_repo(repo: str, rng: random.Random, ids: Ids, window_start: datetime):
    issues, comments, events = [], [], []
    for number in range(1, 61):
        created = SNAPSHOT - timedelta(days=rng.uniform(1, 420))
        author = rng.choice(HUMANS)
        is_pr = rng.random() < 0.3
        labels = rng.sample(LABELS, k=rng.randint(0, 2))
        issue = issue_obj(repo, ids, number, f"Synthetic {'PR' if is_pr else 'issue'} {number}", author,
                          created, labels, is_pr)
        clock = created
        for label in labels:
            clock += timedelta(hours=rng.uniform(0.1, 6))
            events.append(event_obj(ids, issue, "labeled", rng.choice(HUMANS + BOTS), clock, label))
        for _ in range(rng.randint(0, 4)):
            clock += timedelta(hours=rng.expovariate(1 / 30))
            if clock >= SNAPSHOT:
                break
            who = rng.choice(HUMANS + BOTS + [author])
            comments.append(comment_obj(repo, ids, number, who, clock, "x" * rng.randint(10, 800)))
            issue["comments"] += 1
        if rng.random() < 0.6:
            closed = clock + timedelta(hours=rng.choice([2, 12, 60, 300, 1200]) * rng.uniform(0.5, 1.5))
            if closed < SNAPSHOT:
                issue.update(state="closed", state_reason="completed", closed_at=iso(closed))
                events.append(event_obj(ids, issue, "closed", rng.choice(HUMANS), closed))
                clock = closed
                if rng.random() < 0.15:
                    reopened = closed + timedelta(hours=rng.uniform(1, 48))
                    closed_again = reopened + timedelta(hours=rng.uniform(1, 200))
                    if closed_again < SNAPSHOT:
                        events.append(event_obj(ids, issue, "reopened", author, reopened))
                        events.append(event_obj(ids, issue, "closed", rng.choice(HUMANS), closed_again))
                        issue["closed_at"] = iso(closed_again)
                        clock = closed_again
        touch(issue, clock)
        issues.append(issue)

    ws = iso(window_start)
    windowed = [i for i in issues if i["updated_at"] >= ws]
    open_all = [i for i in issues if i["state"] == "open"]
    in_window_numbers = {i["number"] for i in windowed} | {i["number"] for i in open_all}
    comments = [c for c in comments if c["updated_at"] >= ws]
    events = [e for e in events if e["created_at"] >= ws and e["issue"]["number"] in in_window_numbers]
    return {"windowed": windowed, "open": open_all}, comments, sorted(events, key=lambda e: e["created_at"],
                                                                        reverse=True)


def sandbox_repo(repo: str, ids: Ids):
    """Recent, mostly-open synthetic issues for the sandbox repo in the fixture."""
    base = SNAPSHOT - timedelta(days=6)
    issues, comments, events = [], [], []
    for number in range(1, 9):
        created = base + timedelta(hours=number * 3)
        issue = issue_obj(repo, ids, number, f"[seed] Synthetic issue {number}", "Darren-Murphy-mtn", created,
                          ["bug"] if number % 2 else ["enhancement"], False)
        issues.append(issue)
    comments.append(comment_obj(repo, ids, 2, "dev_01", base + timedelta(days=1), "Can reproduce."))
    touch(issues[1], base + timedelta(days=1))
    # issue 3 was already flagged by a previous sync run
    comments.append(comment_obj(repo, ids, 3, "Darren-Murphy-mtn", base + timedelta(days=2), f"{MARKER}\nStale."))
    events.append(event_obj(ids, issues[2], "labeled", "Darren-Murphy-mtn", base + timedelta(days=2), "stale"))
    touch(issues[2], base + timedelta(days=2))
    # issue 4 closed, issue 5 closed then reopened
    closed = base + timedelta(days=3)
    issues[3].update(state="closed", state_reason="completed", closed_at=iso(closed))
    events.append(event_obj(ids, issues[3], "closed", "Darren-Murphy-mtn", closed))
    touch(issues[3], closed)
    events.append(event_obj(ids, issues[4], "closed", "Darren-Murphy-mtn", closed))
    events.append(event_obj(ids, issues[4], "reopened", "Darren-Murphy-mtn", closed + timedelta(hours=5)))
    touch(issues[4], closed + timedelta(hours=5))
    open_all = [i for i in issues if i["state"] == "open"]
    return {"windowed": issues, "open": open_all}, comments, sorted(events, key=lambda e: e["created_at"],
                                                                     reverse=True)


def write_repo(run_dir: Path, repo: str, window_start: datetime, issue_streams: dict, comments: list,
               events: list) -> None:
    repo_dir = run_dir / repo_slug(repo)
    pages, records = {}, {}
    for entity, stream, rows in [("issues", "windowed", issue_streams["windowed"]),
                                 ("issues", "open", issue_streams["open"]),
                                 ("comments", "windowed", comments), ("events", "windowed", events)]:
        (repo_dir / entity).mkdir(parents=True, exist_ok=True)
        (repo_dir / entity / f"page_{stream}_0001.json").write_text(json.dumps(rows))
        pages[f"{entity}:{stream}"], records[f"{entity}:{stream}"] = 1, len(rows)
    manifest = {"repo_full_name": repo, "run_id": RUN_ID, "window_start": iso(window_start),
                "extracted_at": iso(SNAPSHOT), "truncated": False, "pages": pages, "records": records}
    (repo_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", type=Path, default=ROOT / "data" / "sample" / "json")
    ap.add_argument("--as-of", type=datetime.fromisoformat,
                    help="anchor the fixture to this UTC time instead of now, e.g. 2026-09-01T00:00")
    args = ap.parse_args()
    if args.as_of:
        global SNAPSHOT, RUN_ID
        SNAPSHOT = args.as_of.replace(tzinfo=args.as_of.tzinfo or UTC)
        RUN_ID = SNAPSHOT.strftime("%Y%m%dT%H%M%SZ")
    cfg = load_config()
    rng, ids = random.Random(42), Ids()
    window_start = SNAPSHOT - timedelta(days=WINDOW_DAYS)
    run_dir = args.out / "runs" / RUN_ID
    for repo in ("example-org/widgets", "example-org/gadgets"):
        write_repo(run_dir, repo, window_start, *public_repo(repo, rng, ids, window_start))
    write_repo(run_dir, cfg.sandbox_repo, window_start, *sandbox_repo(cfg.sandbox_repo, ids))
    print(f"fixture written to {run_dir} (snapshot {iso(SNAPSHOT)})")


if __name__ == "__main__":
    main()
