"""InnerTube search client (tombulled/innertube, as in the paper) plus retry.

Unauthenticated WEB client, no cookies or accounts. Every failure is treated
as transient and retried with exponential backoff and full jitter, so
workers keep running; rate-limit responses back off on a longer scale.
"""

from __future__ import annotations

import random
import threading
from dataclasses import dataclass

import httpx

RATE_LIMIT_KINDS = {"http_429", "http_403", "api_429", "api_403"}


class SearchError(Exception):
    def __init__(self, kind: str, message: str) -> None:
        super().__init__(f"{kind}: {message}")
        self.kind = kind


def _walk_video_ids(node, out: list[str]) -> None:
    if isinstance(node, dict):
        renderer = node.get("videoRenderer") or node.get("reelItemRenderer")
        if isinstance(renderer, dict) and isinstance(renderer.get("videoId"), str):
            out.append(renderer["videoId"])
        for value in node.values():
            _walk_video_ids(value, out)
    elif isinstance(node, list):
        for value in node:
            _walk_video_ids(value, out)


def extract_video_ids(response: dict) -> list[str]:
    out: list[str] = []
    _walk_video_ids(response, out)
    return out


def _raise_for_status(response: httpx.Response) -> None:
    response.raise_for_status()


class InnertubeSearch:
    def __init__(self, base_url: str | None = None, timeout: float = 20.0) -> None:
        import innertube

        self._client = innertube.InnerTube("WEB")
        session = self._client.adaptor.session
        if base_url:
            session = httpx.Client(base_url=base_url)
            self._client.adaptor.session = session
        session.timeout = httpx.Timeout(timeout)
        # innertube never checks the HTTP status; an HTML 429 page would only
        # surface as "Expected JSON response" and lose the status code.
        session.event_hooks = {"request": [], "response": [_raise_for_status]}

    def search(self, query: str) -> list[str]:
        import innertube.errors

        try:
            response = self._client.search(query=query)
        except httpx.HTTPStatusError as exc:
            code = exc.response.status_code
            kind = f"http_{code}" if code in (403, 429) else f"http_{code // 100}xx"
            raise SearchError(kind, str(exc)) from exc
        except httpx.TransportError as exc:
            raise SearchError("network", repr(exc)) from exc
        except innertube.errors.RequestError as exc:
            raise SearchError(f"api_{getattr(exc.error, 'code', 'x')}", str(exc)) from exc
        except (innertube.errors.ResponseError, ValueError) as exc:
            raise SearchError("bad_response", str(exc)) from exc
        ids = extract_video_ids(response)
        if not ids:
            # Live pilot: a query with no matches still returns ~4 filler
            # videos; a fully empty page is transient, and the same query
            # usually returns results (including hits) on retry.
            raise SearchError("empty_response", "no videoRenderer in response")
        return ids


@dataclass
class Backoff:
    base: float = 1.0
    cap: float = 300.0
    rate_limit_base: float = 30.0
    rate_limit_cap: float = 1800.0
    max_attempts: int = 12
    empty_retries: int = 3

    def delay(self, attempt: int, kind: str, rng: random.Random) -> float:
        if kind in RATE_LIMIT_KINDS:
            ceiling = min(self.rate_limit_cap, self.rate_limit_base * 2**attempt)
        else:
            ceiling = min(self.cap, self.base * 2**attempt)
        return rng.uniform(0, ceiling)


def search_with_retry(backend, query: str, backoff: Backoff, stop: threading.Event,
                      rng: random.Random, on_error=None) -> list[str] | None:
    """Return results, or None if attempts ran out or a stop was requested.

    A query still empty after empty_retries is abandoned (None), not
    counted as zero results: in the live pilot, empties ran at 25% of
    attempts alongside HTTP 403s, i.e. they behave like soft throttling,
    and counting them as searched-with-no-hit would bias the estimate low.
    """
    empties = 0
    for attempt in range(backoff.max_attempts):
        if stop.is_set():
            return None
        try:
            return backend.search(query)
        except SearchError as exc:
            kind = exc.kind
            if kind == "empty_response":
                empties += 1
                if on_error:
                    on_error(kind)
                if empties > backoff.empty_retries:
                    return None
                if stop.wait(backoff.delay(empties, kind, rng)):
                    return None
                continue
        except Exception as exc:  # a worker must never die on one bad response
            kind = f"other_{type(exc).__name__}"
        if on_error:
            on_error(kind)
        if stop.wait(backoff.delay(attempt, kind, rng)):
            return None
    return None
