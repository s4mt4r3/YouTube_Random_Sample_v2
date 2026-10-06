"""Fake InnerTube search server with a planted hit rate.

A case-insensitive ID "exists" iff sha1(id.lower()) % hit_den == 0. Its
canonical case is derived from the hash, so the client must match
case-insensitively. Each response also carries decoy videos that never
match, plus optional injected 429/500 failures.

Run standalone: python -m tests.mock_server --port 8765 --hit-den 4096
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_SPLIT = re.compile(r"\s+(?:OR|\|)\s+")


def _h(s: str) -> int:
    return int.from_bytes(hashlib.sha1(s.lower().encode()).digest()[:8], "big")


def exists(vid: str, hit_den: int) -> bool:
    return len(vid) == 11 and _h(vid) % hit_den == 0


def canonical(vid: str) -> str:
    bits = _h(vid + "#case")
    return "".join(c.upper() if (bits >> i) & 1 else c.lower() for i, c in enumerate(vid[:10])) + vid[10]


class MockState:
    def __init__(self, hit_den: int = 64, fail_rate: float = 0.0, decoys: int = 5, seed: int = 0):
        self.hit_den = hit_den
        self.fail_rate = fail_rate
        self.decoys = decoys
        self.rng = random.Random(seed)
        self.lock = threading.Lock()
        self.requests = 0
        self.queries: list[str] = []


def make_handler(state: MockState):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code: int, body: dict | str, ctype="application/json"):
            data = body.encode() if isinstance(body, str) else json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length) or b"{}")
            with state.lock:
                state.requests += 1
                fail = state.rng.random() < state.fail_rate
                kind = state.rng.choice([429, 500])
                decoy_ids = ["".join(state.rng.choice("0123456789-_") for _ in range(10)) + "A"
                             for _ in range(state.decoys)]
            if fail:
                return self._send(kind, "<html>rate limited</html>", "text/html")
            query = body.get("query", "")
            with state.lock:
                state.queries.append(query)
            tokens = [t.strip('"') for t in _SPLIT.split(query.strip()) if t]
            found = [canonical(t) for t in tokens if exists(t, state.hit_den)]
            vids = decoy_ids[:2] + found + decoy_ids[2:]
            items = [{"videoRenderer": {"videoId": v}} for v in vids]
            self._send(200, {
                "responseContext": {"visitorData": "mock"},
                "contents": {"twoColumnSearchResultsRenderer": {"primaryContents": {
                    "sectionListRenderer": {"contents": [{"itemSectionRenderer": {"contents": items}}]}}}},
            })

    return Handler


class MockServer:
    def __init__(self, **kwargs):
        self.state = MockState(**kwargs)
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(self.state))
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.httpd.server_address[1]}/youtubei/v1/"

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.httpd.shutdown()
        self.httpd.server_close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--hit-den", type=int, default=4096)
    ap.add_argument("--fail-rate", type=float, default=0.0)
    a = ap.parse_args()
    st = MockState(hit_den=a.hit_den, fail_rate=a.fail_rate)
    srv = ThreadingHTTPServer(("127.0.0.1", a.port), make_handler(st))
    print(f"mock InnerTube on http://127.0.0.1:{a.port}/youtubei/v1/ (hit_den={a.hit_den})")
    srv.serve_forever()
