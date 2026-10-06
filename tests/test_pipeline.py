import json

import pandas as pd

from enrich.worker import EnrichWorker, classify_error, pick_auto_caption_langs, segment_ranges
from sampler.campaign import create_campaign
from sampler.search import Backoff, InnertubeSearch
from sampler.worker import Worker
from tests.mock_server import MockServer
from tools import merge, size_campaign, status


class FakeProcessor:
    """Stands in for yt-dlp so tests never touch YouTube."""

    def __init__(self, fail_first: set[str] | None = None):
        self.fail_first = set(fail_first or ())
        self.calls = []

    def process(self, vid):
        self.calls.append(vid)
        if vid in self.fail_first:
            self.fail_first.discard(vid)
            raise RuntimeError("ERROR: [youtube] x: Sign in to confirm you're not a bot")
        return {"video_id": vid, "id": vid, "upload_date": "2024" + "0101", "duration": 30,
                "width": 1080, "height": 1920, "aspect_w_over_h": 0.5625,
                "shorts_status": 200, "tags": ["a"], "steps": {"audio": "ok"}}


def test_classify_error_block_before_unavailable():
    assert classify_error("Video unavailable. Sign in to confirm you're not a bot") == "blocked"
    assert classify_error("Private video") == "unavailable"
    assert classify_error("Sign in to confirm your age") == "unavailable"
    assert classify_error("weird") == "error"


def test_segment_ranges_follow_coding_protocol():
    assert segment_ranges(30) == [(0, 30)]
    assert segment_ranges(260) == [(0, 60), (160, 190)]


def test_auto_caption_pick_prefers_original():
    assert pick_auto_caption_langs({"automatic_captions": {"en-orig": [], "en": [], "fr": []}}) == ["en-orig"]
    assert pick_auto_caption_langs({"automatic_captions": {"de": []}, "language": "de"}) == ["de"]
    assert pick_auto_caption_langs({}) == []


def test_full_pipeline_against_mock(tmp_path):
    prior = tmp_path / "prior.txt"
    create_campaign(tmp_path / "c", 1, 10**9, "OR", campaign_id=11, prior_dataset=str(prior))
    camp = tmp_path / "c"
    with MockServer(hit_den=32) as srv:
        for task in range(3):
            Worker(camp, task, threads=3, max_queries=60, heartbeat_every=0.2,
                   backend_factory=lambda: InnertubeSearch(base_url=srv.base_url),
                   backoff=Backoff(base=0.001)).run()
    from sampler.campaign import shard_totals

    hits = shard_totals(camp)["hits"]
    assert hits
    prior.write_text(hits[0]["video_id"] + "\n")

    first = sorted({h["video_id"] for h in hits})[0]
    proc = FakeProcessor(fail_first={first})
    ew = EnrichWorker(camp, processor=proc, threads=2)
    ew.run(once=True)
    assert ew.counts["blocked"] == 1
    EnrichWorker(camp, processor=proc, threads=2).run(once=True)  # retries only the blocked one
    assert proc.calls.count(first) == 2
    assert len(proc.calls) == len({h["video_id"] for h in hits}) + 1

    s = status.collect(camp)
    assert s["total_queries"] == 180 and s["download_backlog"] == 0
    assert s["shorts_signals"]["vertical(w/h<1)"] == s["unique_videos"]

    summary = merge.merge(camp)
    v = pd.read_parquet(camp / "merged" / "videos.parquet")
    assert v["video_id"].is_unique and len(v) == summary["unique_videos"]
    assert summary["in_prior_sample"] == 1
    assert (v["enrich_status"] == "ok").all()
    assert summary["years"][0]["year"] == 2024
    assert json.loads((camp / "merged" / "estimate.json").read_text())["estimate"]["hits"] == len(hits)


def test_size_campaign_picks_binding_limit():
    plan = size_campaign.size({"max_array_size": 1001, "qos_max_jobs_pu": 504, "qos_max_cpus_pu": 600,
                               "partition_max_time_min": 4320, "qos_max_wall_min": None,
                               "partition": "main"}, cpus_per_task=2)
    assert plan["array_size"] == 296 and plan["binding_limit"].startswith("qos_max_cpus_pu")
    assert plan["time_limit"] == "3-00:00:00" and plan["restarts_per_task_for_campaign"] == 3
