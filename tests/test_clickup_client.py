import pytest

from pipeline_common.clickup_client import ClickUpClient, ClickUpError
from tests.conftest import FakeSession, make_response


def test_authorization_header_is_the_raw_token(clock):
    session = FakeSession(lambda m, u, k: make_response(200, {"tasks": []}))
    ClickUpClient("pk_test_token", session=session, sleep=clock.sleep, clock=clock.time).iter_task_pages("9")
    assert session.headers["Authorization"] == "pk_test_token"
    assert not session.headers["Authorization"].startswith("Bearer")


def test_task_pages_include_closed_and_stop_on_a_short_page(clock):
    pages = {
        0: [{"id": str(i)} for i in range(100)],
        1: [{"id": "last"}],
    }

    def route(method, url, kw):
        assert method == "GET"
        assert kw["params"]["include_closed"] == "true"
        return make_response(200, {"tasks": pages[kw["params"]["page"]]})

    session = FakeSession(route)
    client = ClickUpClient("pk_test", session=session, sleep=clock.sleep, clock=clock.time)
    assert [len(page) for page in client.iter_task_pages("9")] == [100, 1]
    assert len(session.calls) == 2


def test_sleeps_until_reset_on_429(clock):
    state = {"n": 0}

    def route(method, url, kw):
        state["n"] += 1
        if state["n"] == 1:
            return make_response(429, {"err": "Rate limit exceeded"}, {"X-RateLimit-Reset": str(clock.now + 30)})
        return make_response(200, {"id": "t", "url": "https://app.clickup.com/t/t"})

    session = FakeSession(route)
    client = ClickUpClient("pk_test", session=session, sleep=clock.sleep, clock=clock.time)
    created = client.create_task("9", "name", "desc")
    assert created["id"] == "t"
    assert clock.slept == [31]


def test_server_error_on_a_create_is_not_retried(clock):
    """A 5xx create may already have been applied; replaying it would make a second task."""
    attempts = []

    def route(method, url, kw):
        attempts.append(method)
        return make_response(502, {"err": "Bad gateway"})

    session = FakeSession(route)
    client = ClickUpClient("pk_test", session=session, sleep=clock.sleep, clock=clock.time)
    with pytest.raises(ClickUpError, match="may already have been applied") as err:
        client.create_task("9", "name", "desc")
    assert err.value.status == 502
    assert attempts == ["POST"]  # sent exactly once
    assert clock.slept == []


def test_server_errors_on_reads_are_still_retried(clock):
    responses = iter([make_response(503, {}), make_response(200, {"tasks": []})])
    session = FakeSession(lambda m, u, k: next(responses))
    client = ClickUpClient("pk_test", session=session, sleep=clock.sleep, clock=clock.time)
    assert list(client.iter_task_pages("9")) == [[]]
    assert clock.slept == [1]
