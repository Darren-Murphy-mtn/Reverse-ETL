import pytest

from pipeline_common.github_client import GitHubClient, GitHubError, parse_next_link
from tests.conftest import FakeSession, make_response


def client_for(router, clock, **kw):
    session = FakeSession(router)
    return GitHubClient(session=session, sleep=clock.sleep, clock=clock.time, **kw), session


def test_parse_next_link():
    header = ('<https://api.github.com/x?page=2>; rel="next", '
              '<https://api.github.com/x?page=9>; rel="last"')
    assert parse_next_link(header) == "https://api.github.com/x?page=2"
    assert parse_next_link('<https://api.github.com/x?page=1>; rel="prev"') is None
    assert parse_next_link(None) is None


def test_paginate_follows_link_header_and_drops_params_after_first_page(clock):
    pages = {
        "https://api.github.com/r": make_response(
            200, [1, 2], {"Link": '<https://api.github.com/r?page=2>; rel="next"'}),
        "https://api.github.com/r?page=2": make_response(200, [3], {}),
    }
    client, session = client_for(lambda m, url, kw: pages[url], clock)
    assert list(client.paginate("/r", {"per_page": 100})) == [[1, 2], [3]]
    assert session.calls[0][2]["params"] == {"per_page": 100}
    assert session.calls[1][2]["params"] is None


def test_sleeps_until_reset_when_remaining_is_low(clock):
    reset = clock.now + 120
    client, _ = client_for(
        lambda m, u, k: make_response(200, [], {"X-RateLimit-Remaining": "2", "X-RateLimit-Reset": str(reset)}),
        clock, min_remaining=5)
    client.get_json("/r")
    assert clock.slept == [121]


def test_primary_rate_limit_403_waits_then_retries(clock):
    responses = iter([
        make_response(403, {"message": "API rate limit exceeded"},
                      {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(clock.now + 30)}),
        make_response(200, {"ok": True}, {"X-RateLimit-Remaining": "4999", "X-RateLimit-Reset": "0"}),
    ])
    client, _ = client_for(lambda m, u, k: next(responses), clock)
    assert client.get_json("/r") == {"ok": True}
    assert clock.slept == [31]


def test_secondary_rate_limit_honours_retry_after(clock):
    responses = iter([make_response(429, {}, {"Retry-After": "7"}), make_response(200, {"ok": True})])
    client, _ = client_for(lambda m, u, k: next(responses), clock)
    client.get_json("/r")
    assert clock.slept == [7.0]


def test_retries_server_errors_with_backoff(clock):
    responses = iter([make_response(502), make_response(503), make_response(200, {"ok": True})])
    client, _ = client_for(lambda m, u, k: next(responses), clock)
    assert client.get_json("/r") == {"ok": True}
    assert clock.slept == [1, 2]


def test_client_errors_raise_with_status(clock):
    client, _ = client_for(lambda m, u, k: make_response(404, {"message": "Not Found"}), clock)
    with pytest.raises(GitHubError) as err:
        client.get_json("/r")
    assert err.value.status == 404


def test_refuses_to_wait_past_cap(clock):
    client, _ = client_for(
        lambda m, u, k: make_response(
            403, {}, {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(clock.now + 7200)}),
        clock, max_wait_seconds=3600)
    with pytest.raises(GitHubError, match="over the configured cap"):
        client.get_json("/r")
