import json
from datetime import UTC, datetime

from extract.github_extract import Stream, extract_repo, iso, run_stream
from pipeline_common.github_client import GitHubClient
from tests.conftest import FakeSession, make_response

WINDOW = datetime(2026, 6, 1, tzinfo=UTC)


def event(ts: str) -> dict:
    return {"id": hash(ts), "created_at": ts}


def test_events_stream_stops_once_past_window(tmp_path, clock):
    link = lambda n: {"Link": f'<https://api.github.com/e?page={n}>; rel="next"'}  # noqa: E731
    pages = {
        "https://api.github.com/e": make_response(200, [event("2026-09-01T00:00:00Z")], link(2)),
        "https://api.github.com/e?page=2": make_response(200, [event("2026-05-30T00:00:00Z")], link(3)),
        "https://api.github.com/e?page=3": make_response(200, [event("2026-01-01T00:00:00Z")]),
    }
    session = FakeSession(lambda m, url, kw: pages[url])
    client = GitHubClient(session=session, sleep=clock.sleep, clock=clock.time)
    pages_read, records, truncated = run_stream(
        client, Stream("events", "windowed", "/e", {}, stop_before=WINDOW), tmp_path, None)
    assert (pages_read, records, truncated) == (2, 2, False)
    assert len(session.calls) == 2  # page 3 never requested
    assert sorted(p.name for p in tmp_path.iterdir()) == ["page_windowed_0001.json", "page_windowed_0002.json"]


def test_extract_repo_writes_raw_pages_and_manifest_last(tmp_path, clock):
    session = FakeSession(lambda m, url, kw: make_response(200, [{"id": 1, "created_at": iso(WINDOW)}]))
    client = GitHubClient(session=session, sleep=clock.sleep, clock=clock.time)
    run_dir = tmp_path / "20260901T000000Z"
    result = extract_repo(client, "octo/repo.js", run_dir, WINDOW, max_pages=1)
    repo_dir = run_dir / "octo__repo.js"
    manifest = json.loads((repo_dir / "manifest.json").read_text())
    assert manifest["repo_full_name"] == "octo/repo.js"
    assert manifest["window_start"] == "2026-06-01T00:00:00Z"
    assert set(result.pages) == {"issues:windowed", "issues:open", "comments:windowed", "events:windowed"}
    assert (repo_dir / "issues" / "page_open_0001.json").exists()
    # the windowed issue pull filters by `since`; the open pull must not
    params = [kw["params"] for _, url, kw in session.calls if url.endswith("/issues")]
    assert {"since" in p for p in params} == {True, False}
