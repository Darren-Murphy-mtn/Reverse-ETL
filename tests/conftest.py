from __future__ import annotations

import json
from collections.abc import Callable

import pytest
import requests


def make_response(status: int = 200, body=None, headers: dict | None = None) -> requests.Response:
    resp = requests.Response()
    resp.status_code = status
    resp._content = json.dumps(body if body is not None else {}).encode()
    resp.headers.update(headers or {})
    return resp


class FakeSession:
    """Records every request and answers from a router(method, url, kwargs) -> Response."""

    def __init__(self, router: Callable[[str, str, dict], requests.Response]):
        self.headers: dict = {}
        self.calls: list[tuple[str, str, dict]] = []
        self.router = router

    def request(self, method, url, timeout=None, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.router(method, url, kwargs)


class Clock:
    def __init__(self, now: float = 1_000_000.0):
        self.now = now
        self.slept: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


@pytest.fixture
def clock() -> Clock:
    return Clock()
