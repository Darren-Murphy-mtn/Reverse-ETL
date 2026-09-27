"""Minimal GitHub REST client: Link-header pagination, rate-limit awareness, retries."""
from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator
from typing import Any

import requests

API_ROOT = "https://api.github.com"
log = logging.getLogger(__name__)


class GitHubError(RuntimeError):
    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


def parse_next_link(link_header: str | None) -> str | None:
    if not link_header:
        return None
    for part in link_header.split(","):
        url, *params = part.split(";")
        if any(p.strip() == 'rel="next"' for p in params):
            return url.strip().strip("<>")
    return None


class GitHubClient:
    def __init__(
        self,
        token: str | None = None,
        session: requests.Session | None = None,
        min_remaining: int = 5,
        max_retries: int = 5,
        max_wait_seconds: float = 3900,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.time,
    ):
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "reverse-etl-portfolio",
            }
        )
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"
        self.min_remaining = min_remaining
        self.max_retries = max_retries
        self.max_wait_seconds = max_wait_seconds
        self._sleep = sleep
        self._clock = clock

    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        if not url.startswith("http"):
            url = API_ROOT + url
        for attempt in range(self.max_retries + 1):
            resp = self.session.request(method, url, timeout=30, **kwargs)
            if self._is_rate_limited(resp):
                self._wait(self._rate_limit_wait(resp), f"rate limited ({resp.status_code})")
                continue
            if resp.status_code >= 500:
                self._wait(min(2**attempt, 60), f"server error {resp.status_code}")
                continue
            self._throttle(resp)
            if not resp.ok:
                raise GitHubError(
                    f"{method} {url} -> {resp.status_code}: {resp.text[:300]}", resp.status_code
                )
            return resp
        raise GitHubError(f"{method} {url}: retries exhausted after {self.max_retries} attempts")

    def get_json(self, url: str, params: dict | None = None) -> Any:
        return self.request("GET", url, params=params).json()

    def post_json(self, url: str, payload: dict) -> Any:
        return self.request("POST", url, json=payload).json()

    def paginate(self, url: str, params: dict | None = None) -> Iterator[list[dict]]:
        """Yield one page (list of records) at a time, following the Link header."""
        next_url: str | None = url
        next_params = params
        while next_url:
            resp = self.request("GET", next_url, params=next_params)
            yield resp.json()
            next_url = parse_next_link(resp.headers.get("Link"))
            next_params = None  # the next link already carries the query string

    @staticmethod
    def _is_rate_limited(resp: requests.Response) -> bool:
        if resp.status_code == 429:
            return True
        if resp.status_code != 403:
            return False
        return (
            resp.headers.get("X-RateLimit-Remaining") == "0"
            or "Retry-After" in resp.headers
            or "rate limit" in resp.text.lower()
        )

    def _rate_limit_wait(self, resp: requests.Response) -> float:
        if "Retry-After" in resp.headers:
            return float(resp.headers["Retry-After"])
        if resp.headers.get("X-RateLimit-Remaining") == "0" and "X-RateLimit-Reset" in resp.headers:
            return max(float(resp.headers["X-RateLimit-Reset"]) - self._clock(), 0) + 1
        return 60.0  # GitHub's guidance for secondary limits without a Retry-After

    def _throttle(self, resp: requests.Response) -> None:
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset = resp.headers.get("X-RateLimit-Reset")
        if remaining is None or reset is None or int(remaining) > self.min_remaining:
            return
        self._wait(max(float(reset) - self._clock(), 0) + 1, f"only {remaining} requests left")

    def _wait(self, seconds: float, reason: str) -> None:
        if seconds > self.max_wait_seconds:
            raise GitHubError(f"{reason}: would need to wait {seconds:.0f}s, over the configured cap")
        log.warning("%s; sleeping %.0fs", reason, seconds)
        self._sleep(seconds)
