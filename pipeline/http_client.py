from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from pipeline.rate_limit import GlobalRateLimiter


RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class ApiBudgetExceededError(RuntimeError):
    pass


class ApiRequestError(RuntimeError):
    pass


@dataclass
class RequestBudget:
    max_requests: int
    used_requests: int = 0

    def consume(self, label: str) -> None:
        self.used_requests += 1
        if self.used_requests > self.max_requests:
            raise ApiBudgetExceededError(
                f"API request budget exceeded while calling [{label}] "
                f"({self.used_requests}/{self.max_requests})."
            )


def _retry_after_seconds(header_value: str | None) -> float | None:
    if not header_value:
        return None
    try:
        seconds = float(header_value)
        return max(0.0, seconds)
    except ValueError:
        pass
    try:
        target = parsedate_to_datetime(header_value)
        now = datetime.now(timezone.utc)
        if target.tzinfo is None:
            target = target.replace(tzinfo=timezone.utc)
        delta = (target - now).total_seconds()
        return max(0.0, delta)
    except (TypeError, ValueError):
        return None


class SafeHttpClient:
    def __init__(
        self,
        *,
        rate_limiter: GlobalRateLimiter,
        timeout_seconds: int,
        max_retries: int,
        base_backoff_seconds: float,
        request_budget: RequestBudget,
        user_agent: str,
    ) -> None:
        self.rate_limiter = rate_limiter
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.base_backoff_seconds = base_backoff_seconds
        self.request_budget = request_budget
        self.user_agent = user_agent

    def get_json(self, url: str, *, label: str) -> Any:
        total_attempts = self.max_retries + 1
        for attempt in range(1, total_attempts + 1):
            self.request_budget.consume(label)
            self.rate_limiter.wait_for_slot(label=label)

            req = Request(
                url=url,
                method="GET",
                headers={
                    "Accept": "application/json",
                    "User-Agent": self.user_agent,
                },
            )
            try:
                with urlopen(req, timeout=self.timeout_seconds) as response:
                    body = response.read().decode("utf-8")
                return json.loads(body)
            except HTTPError as exc:
                status = exc.code
                if status not in RETRYABLE_STATUS_CODES or attempt >= total_attempts:
                    raise ApiRequestError(
                        f"[{label}] HTTP {status} request failed after attempt "
                        f"{attempt}/{total_attempts}."
                    ) from exc
                retry_after = _retry_after_seconds(exc.headers.get("Retry-After"))
                if retry_after is None:
                    retry_after = self.base_backoff_seconds * (2 ** (attempt - 1))
                    logging.warning(
                        "[%s] HTTP %s retry %s/%s with exponential backoff %.1fs",
                        label,
                        status,
                        attempt,
                        self.max_retries,
                        retry_after,
                    )
                else:
                    logging.warning(
                        "[%s] HTTP %s retry %s/%s honoring Retry-After %.1fs",
                        label,
                        status,
                        attempt,
                        self.max_retries,
                        retry_after,
                    )
                time.sleep(retry_after)
            except URLError as exc:
                if attempt >= total_attempts:
                    raise ApiRequestError(
                        f"[{label}] network error after attempt {attempt}/{total_attempts}: {exc}"
                    ) from exc
                retry_after = self.base_backoff_seconds * (2 ** (attempt - 1))
                logging.warning(
                    "[%s] network error retry %s/%s in %.1fs: %s",
                    label,
                    attempt,
                    self.max_retries,
                    retry_after,
                    exc,
                )
                time.sleep(retry_after)

        raise ApiRequestError(f"[{label}] request failed after {total_attempts} attempts.")
