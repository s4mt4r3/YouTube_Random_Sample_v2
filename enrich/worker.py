"""Hit processing, run separately from search (the hit shards are the queue).

For each hit, as in the paper: yt-dlp extract_info (raw info dict saved as
gzipped JSON plus a flat record), any available transcripts, and the audio
track. Study extension: a /shorts/<id> probe without redirects, duration,
and width/height, recorded raw; optionally the first 60 s plus 30 s from
the midpoint of the remainder (--fetch-video-segments).

Status records are append-only JSONL, one file per enrich worker. "ok" and
"unavailable" are terminal; "blocked" and "error" are retried later. Block
patterns are checked before unavailable patterns because YouTube's bot
check was previously misread as "unavailable" (see skills.md).
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import random
import re
import signal
import sys
import threading
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import requests

from sampler.campaign import deadline_of, iso, load_manifest, read_jsonl, utcnow

BLOCK_RE = re.compile(r"sign in to confirm|not a bot|http error 429|too many requests|captcha", re.I)
UNAVAILABLE_RE = re.compile(
    r"video unavailable|private video|has been removed|no longer available|account associated|"
    r"terminated|members-only|join this channel|copyright|age.restricted|sign in to confirm your age|"
    r"live event will begin|premiere", re.I)

FLAT_FIELDS = [
    "id", "channel_id", "channel", "uploader_id", "title", "description", "tags", "categories",
    "upload_date", "timestamp", "duration", "view_count", "like_count", "comment_count",
    "live_status", "was_live", "is_live", "availability", "age_limit", "language", "media_type",
    "channel_follower_count", "width", "height", "fps", "aspect_ratio", "playable_in_embed",
]


def flat_record(info: dict) -> dict:
    rec = {k: info.get(k) for k in FLAT_FIELDS}
    rec["subscriber_count"] = info.get("channel_follower_count")
    rec["caption_langs"] = sorted((info.get("subtitles") or {}).keys())
    rec["auto_caption_langs"] = sorted((info.get("automatic_captions") or {}).keys())
    w, h = info.get("width"), info.get("height")
    rec["aspect_w_over_h"] = (w / h) if w and h else None
    return rec


def classify_error(msg: str) -> str:
    if BLOCK_RE.search(msg) and "your age" not in msg.lower():
        return "blocked"
    if UNAVAILABLE_RE.search(msg):
        return "unavailable"
    return "error"


def probe_shorts(video_id: str, session: requests.Session, timeout: float = 15) -> dict:
    try:
        r = session.get(f"https://www.youtube.com/shorts/{video_id}", allow_redirects=False, timeout=timeout)
        return {"shorts_status": r.status_code, "shorts_location": r.headers.get("Location")}
    except requests.RequestException as exc:
        return {"shorts_status": None, "shorts_location": None, "shorts_error": repr(exc)}


def segment_ranges(duration: float | None) -> list[tuple[float, float]]:
    """First 60 s, then 30 s starting at the midpoint of what follows."""
    if not duration or duration <= 60:
        return [(0, duration or 60)]
    mid = 60 + (duration - 60) / 2
    return [(0, 60), (mid, min(mid + 30, duration))]


def pick_auto_caption_langs(info: dict) -> list[str]:
    # Downloading every auto-translated track would mean 100+ requests per
    # video; the original-language ASR track is the transcript.
    auto = info.get("automatic_captions") or {}
    orig = [k for k in auto if k.endswith("-orig")]
    if orig:
        return orig
    lang = info.get("language")
    return [lang] if lang and lang in auto else []


class YtDlpProcessor:
    def __init__(self, out_dir: Path, audio: bool = True, subs: bool = True,
                 segments_dir: Path | None = None) -> None:
        self.out = out_dir
        self.audio = audio
        self.subs = subs
        self.segments_dir = segments_dir
        for sub in ("info", "subs", "audio"):
            (out_dir / sub).mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()

    def _opts(self, **extra) -> dict:
        return {"quiet": True, "no_warnings": True, "noprogress": True, "retries": 3, **extra}

    def process(self, video_id: str) -> dict:
        import yt_dlp

        url = f"https://www.youtube.com/watch?v={video_id}"
        rec: dict = {"video_id": video_id}
        with yt_dlp.YoutubeDL(self._opts(skip_download=True)) as ydl:
            info = ydl.sanitize_info(ydl.extract_info(url, download=False))
        with gzip.open(self.out / "info" / f"{video_id}.info.json.gz", "wt") as fh:
            json.dump(info, fh)
        rec.update(flat_record(info))
        rec.update(probe_shorts(video_id, self.session))

        steps = {}
        if self.subs:
            auto = pick_auto_caption_langs(info)
            manual = sorted((info.get("subtitles") or {}).keys())
            if manual or auto:
                try:
                    with yt_dlp.YoutubeDL(self._opts(
                            skip_download=True, writesubtitles=bool(manual), writeautomaticsub=bool(auto),
                            subtitleslangs=manual + auto, subtitlesformat="json3/vtt/best",
                            outtmpl=str(self.out / "subs" / "%(id)s.%(ext)s"))) as ydl:
                        ydl.process_ie_result(info, download=True)
                    steps["subs"] = "ok"
                except Exception as exc:  # transcripts are best-effort
                    steps["subs"] = f"{classify_error(str(exc))}: {str(exc)[:200]}"
            else:
                steps["subs"] = "none_available"
        if self.audio:
            try:
                with yt_dlp.YoutubeDL(self._opts(format="bestaudio/best",
                                                 outtmpl=str(self.out / "audio" / "%(id)s.%(ext)s"))) as ydl:
                    ydl.download([url])
                steps["audio"] = "ok"
            except Exception as exc:
                steps["audio"] = f"{classify_error(str(exc))}: {str(exc)[:200]}"
        if self.segments_dir:
            from yt_dlp.utils import download_range_func

            self.segments_dir.mkdir(parents=True, exist_ok=True)
            ranges = segment_ranges(info.get("duration"))
            try:
                with yt_dlp.YoutubeDL(self._opts(
                        format="bv*[height<=480]+ba/b[height<=480]/b",
                        download_ranges=download_range_func(None, ranges),
                        outtmpl=str(self.segments_dir / "%(id)s.%(section_start)d.%(ext)s"))) as ydl:
                    ydl.download([url])
                steps["segments"] = "ok"
            except Exception as exc:
                steps["segments"] = f"{classify_error(str(exc))}: {str(exc)[:200]}"
            rec["segment_ranges"] = ranges
        rec["steps"] = steps
        return rec


def owned(video_id: str, worker_idx: int, n_workers: int) -> bool:
    return int(hashlib.md5(video_id.encode()).hexdigest(), 16) % n_workers == worker_idx


def queued_ids(campaign_dir: Path) -> list[str]:
    seen: dict[str, str] = {}
    for p in sorted((campaign_dir / "hits").glob("task*_r*.jsonl")):
        for rec in read_jsonl(p):
            if rec.get("type") == "hit" and rec["video_id"] not in seen:
                seen[rec["video_id"]] = rec["found_at"]
    return sorted(seen, key=seen.get)


def status_by_id(enriched_dir: Path) -> dict[str, dict]:
    latest: dict[str, dict] = {}
    for p in sorted((enriched_dir / "status").glob("*.jsonl")):
        for rec in read_jsonl(p):
            latest[rec["video_id"]] = rec
    return latest


class EnrichWorker:
    def __init__(self, campaign_dir: Path, worker_idx: int = 0, n_workers: int = 1,
                 threads: int = 4, processor=None, max_attempts: int = 8, poll_every: float = 60.0,
                 drain_hours: float = 48.0) -> None:
        self.dir = campaign_dir
        self.out = campaign_dir / "enriched"
        (self.out / "status").mkdir(parents=True, exist_ok=True)
        self.worker_idx, self.n_workers = worker_idx, n_workers
        self.threads = threads
        self.processor = processor
        self.max_attempts = max_attempts
        self.poll_every = poll_every
        self.deadline = deadline_of(load_manifest(campaign_dir))
        self.drain_hours = drain_hours
        self.stop = threading.Event()
        self.lock = threading.Lock()
        tag = f"w{worker_idx:03d}_{os.environ.get('SLURM_JOB_ID', 'local')}_{os.getpid()}"
        self.fh = open(self.out / "status" / f"{tag}.jsonl", "a", buffering=1)
        self.counts: Counter = Counter()
        self.block_streak = 0
        self.rng = random.Random()

    def _record(self, rec: dict) -> None:
        with self.lock:
            self.fh.write(json.dumps(rec, separators=(",", ":"), default=str) + "\n")
            self.counts[rec["status"]] += 1

    def _pending(self) -> list[tuple[str, int]]:
        latest = status_by_id(self.out)
        attempts = Counter()
        for p in (self.out / "status").glob("*.jsonl"):
            for rec in read_jsonl(p):
                attempts[rec["video_id"]] += 1
        out = []
        for vid in queued_ids(self.dir):
            if not owned(vid, self.worker_idx, self.n_workers):
                continue
            st = latest.get(vid, {}).get("status")
            if st in ("ok", "unavailable") or attempts[vid] >= self.max_attempts:
                continue
            out.append((vid, attempts[vid]))
        return out

    def _one(self, vid: str, attempt: int) -> None:
        t0 = time.time()
        try:
            rec = self.processor.process(vid)
            rec["status"] = "ok"
            with self.lock:
                self.block_streak = 0
        except Exception as exc:
            msg = str(exc)
            rec = {"video_id": vid, "status": classify_error(msg), "error": msg[:500]}
            if rec["status"] == "blocked":
                with self.lock:
                    self.block_streak += 1
        rec.update(attempt=attempt + 1, processed_at=iso(utcnow()), seconds=round(time.time() - t0, 2),
                   host=os.uname().nodename)
        self._record(rec)

    def run(self, once: bool = False) -> Counter:
        while not self.stop.is_set():
            pending = self._pending()
            for i in range(0, len(pending), self.threads):
                if self.stop.is_set():
                    break
                batch = [threading.Thread(target=self._one, args=p) for p in pending[i:i + self.threads]]
                for t in batch:
                    t.start()
                for t in batch:
                    t.join()
                if self.block_streak >= 3:
                    # Bot-checked: pause with growing, jittered waits instead
                    # of burning every pending video's attempts.
                    wait = min(3600, 60 * 2 ** min(self.block_streak - 3, 6)) * self.rng.uniform(0.5, 1.0)
                    print(f"[{datetime.now():%F %T}] blocked x{self.block_streak}; sleeping {wait:.0f}s", flush=True)
                    if self.stop.wait(wait):
                        break
            print(f"[{datetime.now():%F %T}] pass done pending={len(pending)} counts={dict(self.counts)}", flush=True)
            if once:
                break
            past_deadline = utcnow() >= self.deadline
            if past_deadline and not pending:
                break
            if (utcnow() - self.deadline).total_seconds() > self.drain_hours * 3600:
                break
            self.stop.wait(self.poll_every)
        self.fh.close()
        return self.counts


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m enrich.worker")
    p.add_argument("--campaign-dir", type=Path, required=True)
    p.add_argument("--worker-idx", type=int, default=int(os.environ.get("SLURM_ARRAY_TASK_ID", 0)))
    p.add_argument("--n-workers", type=int, default=1)
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--no-audio", action="store_true")
    p.add_argument("--no-subs", action="store_true")
    p.add_argument("--fetch-video-segments", action="store_true",
                   help="download first 60 s + 30 s from the midpoint of the remainder (needs ffmpeg)")
    p.add_argument("--segments-dir", type=Path,
                   default=Path(os.environ.get("DIAL_SEGMENTS_DIR", f"/scratch/{os.environ.get('USER', 'user')}/dial_segments")))
    p.add_argument("--once", action="store_true", help="one pass over the queue, then exit")
    args = p.parse_args(argv)
    proc = YtDlpProcessor(args.campaign_dir / "enriched", audio=not args.no_audio, subs=not args.no_subs,
                          segments_dir=args.segments_dir if args.fetch_video_segments else None)
    w = EnrichWorker(args.campaign_dir, args.worker_idx, args.n_workers, args.threads, proc)
    for sig in (signal.SIGUSR1, signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda s, f: w.stop.set())
    counts = w.run(once=args.once)
    print(f"enrich done: {dict(counts)}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
