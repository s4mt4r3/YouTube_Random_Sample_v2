import json
import random
import threading
from pathlib import Path

import pytest

from sampler import idgen
from sampler.campaign import claim_restart, create_campaign, read_jsonl, shard_totals
from sampler.estimate import PAPER, size_estimate, year_distribution
from sampler.search import Backoff, InnertubeSearch, SearchError, search_with_retry
from sampler.worker import Worker
from tests.mock_server import MockServer, canonical, exists


# ---- ID generator -----------------------------------------------------------

def test_ids_letters_and_pos11():
    for q in range(200):
        ids = idgen.query_ids(1, 2, 3, q)
        assert len(ids) == 32
        for vid in ids:
            assert len(vid) == 11
            assert all(c in idgen.LETTERS for c in vid[:10])
            assert vid[10] in idgen.POS11
            assert idgen.is_valid_candidate(vid)


def test_ids_distinct_case_insensitively():
    for q in range(500):
        ids = idgen.query_ids(9, 0, 0, q)
        assert len({i.lower() for i in ids}) == 32


def test_generate_ids_redraws_case_duplicates():
    class Rigged:
        # First draw: all 32 rows identical; later draws: real randomness.
        def __init__(self):
            self.calls = 0
            self.real = idgen.query_rng(0, 0, 0, 0)

        def integers(self, lo, hi, size):
            self.calls += 1
            if self.calls <= 2:
                import numpy as np
                return np.zeros(size, dtype=int)
            return self.real.integers(lo, hi, size=size)

    ids = idgen.generate_ids(Rigged())
    assert len({i.lower() for i in ids}) == 32


def test_pos11_set_and_space():
    assert sorted(idgen.POS11) == sorted("AEIMQUYcgkosw048")
    assert len(set(c.lower() for c in idgen.POS11)) == 16
    assert idgen.ID_SPACE == 2**64
    assert idgen.IDS_PER_QUERY * idgen.CASE_FACTOR == 32_768


def test_build_query():
    ids = ["a" * 10 + "A", "b" * 10 + "E"]
    assert idgen.build_query(ids, "OR") == "aaaaaaaaaaA OR bbbbbbbbbbE"
    assert idgen.build_query(ids, "|") == "aaaaaaaaaaA | bbbbbbbbbbE"


# ---- hit validation ---------------------------------------------------------

def test_match_hits_case_insensitive_and_discards_others():
    queried = ["dQw4w9WgXcQ".replace("4", "x").replace("9", "y")[:10] + "Q", "abcdefghijA"]
    target = queried[0].swapcase()[:10] + "Q"
    returned = ["zzzzzzzzzzz", target, "ABCDEFGHIJE", "abcdefghij", target, "ABCDEFGHIJA"]
    hits = idgen.match_hits(queried, returned)
    assert [(h[0], h[1]) for h in hits] == [(target, queried[0]), ("ABCDEFGHIJA", "abcdefghijA")]
    assert hits[0][2] == 1


def test_match_hits_pos11_case_insensitive():
    # Regression: the pilot sends known IDs with position 11 case-flipped too;
    # a strict position-11 check rejected about half of the real matches.
    assert idgen.match_hits(["abcdefghija"], ["ABCDEFGHIJA"]) == [("ABCDEFGHIJA", "abcdefghija", 0)]
    assert idgen.match_hits(["abcdefghijA"], ["abcdefghijE"]) == []


# ---- estimator ----------------------------------------------------------------

def test_size_estimate_reproduces_paper():
    est = size_estimate(PAPER["case_insensitive_ids"] / 32, PAPER["hits"])
    assert est["guesses_per_hit"] == pytest.approx(PAPER["guesses_per_hit"], abs=1)
    assert est["estimated_public_videos"] == pytest.approx(9_881_141_822, rel=1e-9)
    assert est["margin_of_error"] == pytest.approx(0.0196, abs=1e-3)


def test_paper_queries_per_hit():
    assert PAPER["queries_per_hit"] == pytest.approx(56_972, rel=1e-3)


def test_size_estimate_zero_hits():
    assert size_estimate(1000, 0)["estimated_public_videos"] is None


def test_year_distribution_applies_shares():
    rows = year_distribution(["20210101", "20220505", "20220101", None], 1000.0)
    by = {r["year"]: r for r in rows}
    assert by[2022]["pct"] == pytest.approx(200 / 3)
    assert by[2022]["est_uploads"] == pytest.approx(1000 * 2 / 3)
    assert by[2022]["est_cumulative"] == pytest.approx(1000)
    assert by[None]["count"] == 1


# ---- RNG reproducibility and no overlap ---------------------------------------

def test_rng_reproducible():
    assert idgen.query_ids(42, 7, 3, 12345) == idgen.query_ids(42, 7, 3, 12345)


def test_no_overlap_across_workers_restarts_queries():
    seen = set()
    total = 0
    for task in range(20):
        for restart in range(3):
            for q in range(100):
                ids = idgen.query_ids(42, task, restart, q)
                seen.update(i.lower() for i in ids)
                total += len(ids)
    assert len(seen) == total
    assert idgen.query_ids(42, 1, 0, 0) != idgen.query_ids(42, 0, 1, 0)
    assert idgen.query_ids(42, 0, 0, 1) != idgen.query_ids(43, 0, 0, 1)


def test_restart_claims_are_unique(tmp_path):
    create_campaign(tmp_path, 1, 10, "OR", campaign_id=1)
    claims = []
    threads = [threading.Thread(target=lambda: claims.append(claim_restart(tmp_path, 5))) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(claims) == list(range(16))


# ---- retry ------------------------------------------------------------------------

def test_retry_survives_transient_errors():
    class Flaky:
        def __init__(self):
            self.n = 0

        def search(self, q):
            self.n += 1
            if self.n < 4:
                raise SearchError("http_429" if self.n == 1 else "network", "boom")
            return ["x"]

    errs = []
    out = search_with_retry(Flaky(), "q", Backoff(base=0.001, rate_limit_base=0.001), threading.Event(),
                            random.Random(0), on_error=errs.append)
    assert out == ["x"] and errs == ["http_429", "network", "network"]


def test_retry_gives_up_and_returns_none():
    class Dead:
        def search(self, q):
            raise RuntimeError("weird")

    out = search_with_retry(Dead(), "q", Backoff(base=0.0001, max_attempts=3), threading.Event(), random.Random(0))
    assert out is None


# ---- mock backend integration ------------------------------------------------------

def test_mock_planted_hit_found_via_innertube_client():
    with MockServer(hit_den=1) as srv:
        backend = InnertubeSearch(base_url=srv.base_url)
        ids = idgen.query_ids(1, 0, 0, 0)
        returned = backend.search(idgen.build_query(ids, "OR"))
        hits = idgen.match_hits(ids, returned)
        assert len(hits) == 32
        assert all(h[0] == canonical(h[1]) for h in hits)


def _run_worker(tmp_path, srv, task, max_queries, **kw):
    w = Worker(tmp_path, task, threads=4, max_queries=max_queries,
               backend_factory=lambda: InnertubeSearch(base_url=srv.base_url),
               backoff=Backoff(base=0.001, rate_limit_base=0.001), heartbeat_every=0.2, **kw)
    return w, w.run()


def test_worker_end_to_end_against_mock(tmp_path):
    create_campaign(tmp_path, 1, 10**9, "OR", campaign_id=77)
    with MockServer(hit_den=64, fail_rate=0.2) as srv:
        w, final = _run_worker(tmp_path, srv, 3, 300, log_ids=True)
    assert final["queries_done"] == 300
    assert final["errors"]  # injected failures were seen and survived
    totals = shard_totals(tmp_path)
    assert totals["total_queries"] == 300
    # Every recorded hit is regenerable and genuinely planted.
    expected = 0
    for q in range(300):
        expected += sum(exists(i, 64) for i in idgen.query_ids(77, 3, 0, q))
    assert len(totals["hits"]) == final["hits"] == expected
    for h in totals["hits"]:
        assert h["queried_id"] in idgen.query_ids(77, 3, h["restart_idx"], h["query_idx"])
        assert h["video_id"] == canonical(h["queried_id"])
    import gzip
    lines = gzip.open(next((tmp_path / "ids").glob("*.tsv.gz")), "rt").read().splitlines()
    assert len(lines) == 300
    idx, ids = lines[0].split("\t")
    assert ids.split(",") == idgen.query_ids(77, 3, 0, int(idx))


def test_checkpoint_resume_no_duplicates(tmp_path):
    create_campaign(tmp_path, 1, 10**9, "OR", campaign_id=5)
    with MockServer(hit_den=16) as srv:
        w = Worker(tmp_path, 0, threads=4, backend_factory=lambda: InnertubeSearch(base_url=srv.base_url),
                   backoff=Backoff(base=0.001), heartbeat_every=0.1)
        t = threading.Timer(1.0, w.request_stop, args=("USR1",))
        t.start()
        first = w.run()
        assert first["stop_reason"] == "USR1" and first["queries_done"] > 0
        w2, second = _run_worker(tmp_path, srv, 0, 200)
    assert w2.restart_idx == 1
    totals = shard_totals(tmp_path)
    assert totals["total_queries"] == first["queries_done"] + second["queries_done"]
    keys = [(h["task_id"], h["restart_idx"], h["query_idx"], h["video_id"]) for h in totals["hits"]]
    assert len(keys) == len(set(keys))
    vids = [h["video_id"] for h in totals["hits"]]
    assert len(vids) == len(set(vids))  # independent streams: no repeated videos


def test_torn_last_line_is_ignored(tmp_path):
    p = tmp_path / "x.jsonl"
    p.write_text(json.dumps({"type": "hit", "a": 1}) + "\n" + '{"type": "hi')
    assert list(read_jsonl(p)) == [{"type": "hit", "a": 1}]


def test_target_reached_stops_and_writes_stop_file(tmp_path):
    create_campaign(tmp_path, 1, 3, "OR", campaign_id=8)
    with MockServer(hit_den=4) as srv:
        w = Worker(tmp_path, 0, threads=2, backend_factory=lambda: InnertubeSearch(base_url=srv.base_url),
                   heartbeat_every=0.05, stop_check_every=0.1)
        final = w.run()
    assert final["stop_reason"] == "target"
    assert (tmp_path / "STOP").exists()


def test_empty_response_retried_then_abandoned():
    class Empty:
        def __init__(self, succeed_on=None):
            self.n, self.succeed_on = 0, succeed_on

        def search(self, q):
            self.n += 1
            if self.succeed_on and self.n == self.succeed_on:
                return ["hit"]
            raise SearchError("empty_response", "none")

    bo = Backoff(base=0.0001, empty_retries=3)
    assert search_with_retry(Empty(succeed_on=2), "q", bo, threading.Event(), random.Random(0)) == ["hit"]
    e = Empty()
    assert search_with_retry(e, "q", bo, threading.Event(), random.Random(0)) is None
    assert e.n == 4


def test_cap_corrected_estimate_scales_by_searched_ids():
    from sampler.estimate import both_estimates
    e = both_estimates(1_000_000, 20, {"ids_per_query": 32, "searched_ids_per_query": 31})
    ratio = e["cap_corrected"]["estimated_public_videos"] / e["paper_method"]["estimated_public_videos"]
    assert ratio == pytest.approx(32 / 31)


def test_hits_record_query_position(tmp_path):
    create_campaign(tmp_path, 1, 10**9, "OR", campaign_id=3, searched_ids_per_query=31)
    with MockServer(hit_den=8) as srv:
        _run_worker(tmp_path, srv, 0, 50)
    for h in shard_totals(tmp_path)["hits"]:
        assert idgen.query_ids(3, 0, 0, h["query_idx"])[h["queried_pos"]] == h["queried_id"]
