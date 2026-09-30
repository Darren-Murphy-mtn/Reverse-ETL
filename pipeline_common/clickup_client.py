"""ClickUp REST v2 client. The token is sent as the raw Authorization value, with no Bearer prefix."""
from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator
from typing import Any

import requests

API_ROOT = "https://api.clickup.com/api/v2"
PAGE_SIZE = 100
# Safe to replay after a server error. A write is not: ClickUp may have applied it
# before the response was lost, so replaying creates a second task.
IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"})
log = logging.getLogger(__name__)


class ClickUpError(RuntimeError):
    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


class ClickUpClient:
    def __init__(
        self,
        token: str,
        session: requests.Session | None = None,
        min_remaining: int = 15,
        max_retries: int = 5,
        max_wait_seconds: float = 180,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.time,
    ):
        self._token = token
        self.session = session or requests.Session()
        self.session.headers["Authorization"] = token
        self.session.headers["Content-Type"] = "application/json"
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
            if resp.status_code == 429:
                self._wait(self._reset_wait(resp), "rate limited (429)")
                continue
            if resp.status_code >= 500:
                # The dedupe scan in sync_stale_issues runs before each create, so it cannot
                # see a duplicate produced by a retry inside one create. Surface the failure
                # instead; the caller logs it per issue and moves on.
                if method.upper() not in IDEMPOTENT_METHODS:
                    raise ClickUpError(
                        f"{method} {url} -> {resp.status_code}: not retried, the request may "
                        f"already have been applied.",
                        resp.status_code,
                    )
                self._wait(min(2**attempt, 60), f"server error {resp.status_code}")
                continue
            self._throttle(resp)
            if not resp.ok:
                body = self._redact(resp.text[:300])
                raise ClickUpError(f"{method} {url} -> {resp.status_code}: {body}", resp.status_code)
            return resp
        raise ClickUpError(f"{method} {url}: retries exhausted after {self.max_retries} attempts")

    def iter_task_pages(self, list_id: str, max_pages: int = 200) -> Iterator[list[dict]]:
        """Yield pages of tasks, including closed ones. Stops on a short page."""
        for page in range(max_pages):
            resp = self.request(
                "GET",
                f"/list/{list_id}/task",
                params={"page": page, "include_closed": "true", "subtasks": "true"},
            )
            tasks = resp.json().get("tasks") or []
            yield tasks
            if len(tasks) < PAGE_SIZE:
                return
        raise ClickUpError(f"list {list_id}: still full after {max_pages} pages")

    def create_task(self, list_id: str, name: str, description: str) -> dict:
        resp = self.request(
            "POST", f"/list/{list_id}/task", json={"name": name, "description": description}
        )
        return resp.json()

    def _redact(self, text: str) -> str:
        if self._token and self._token in text:
            return text.replace(self._token, "[REDACTED]")
        return text

    def _reset_wait(self, resp: requests.Response) -> float:
        # X-RateLimit-Reset is a unix timestamp. The free-plan window is one minute.
        reset = resp.headers.get("X-RateLimit-Reset")
        if reset is None:
            return 60.0
        return max(float(reset) - self._clock(), 0) + 1

    def _throttle(self, resp: requests.Response) -> None:
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset = resp.headers.get("X-RateLimit-Reset")
        if remaining is None or reset is None or int(remaining) > self.min_remaining:
            return
        self._wait(max(float(reset) - self._clock(), 0) + 1, f"only {remaining} requests left")

    def _wait(self, seconds: float, reason: str) -> None:
        if seconds > self.max_wait_seconds:
            raise ClickUpError(f"{reason}: would need to wait {seconds:.0f}s, over the configured cap")
        log.warning("%s; sleeping %.0fs", reason, seconds)
        self._sleep(seconds)
