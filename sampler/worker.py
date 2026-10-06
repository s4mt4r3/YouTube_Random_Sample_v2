"""Long-running search worker (one per Slurm array task).

Threads pull query indices from a shared counter; each query's 32 IDs are
regenerated from (campaign_id, task_id, restart_idx, query_idx). A query
counts toward the estimator only once its results are processed, and its
hits are written under the same lock that bumps the counter, so hit and
progress records always agree. In-flight queries at shutdown are not
counted.

SIGUSR1/SIGTERM: stop issuing queries, let in-flight ones finish, flush the
shard, write the final checkpoint, exit 0. The Slurm script decides whether
to requeue.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import random
import signal
import socket
import sys
import threading
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

from .campaign import claim_restart, deadline_of, iso, load_manifest, utcnow, write_json_atomic
from .idgen import build_query, match_hits, query_ids
from .search import Backoff, InnertubeSearch, search_with_retry


class Worker:
    def __init__(self, campaign_dir: Path, task_id: int, threads: int = 8,
                 log_ids: bool = False, backend_factory=None, max_queries: int | None = None,
                 heartbeat_every: float = 30.0, stop_check_every: float = 60.0,
                 ids_per_shard: int = 1_000_000, backoff: Backoff | None = None,
                 fsync_every: float = 10.0) -> None:
        self.dir = campaign_dir
        self.manifest = load_manifest(campaign_dir)
        self.campaign_id = int(self.manifest["campaign_id"])
        self.separator = self.manifest["separator"]
        self.n_ids = int(self.manifest.get("ids_per_query", 32))
        self.deadline = deadline_of(self.manifest)
        self.task_id = task_id
        self.threads = threads
        self.log_ids = log_ids
        self.backend_factory = backend_factory or InnertubeSearch
        self.max_queries = max_queries
        self.heartbeat_every = heartbeat_every
        self.stop_check_every = stop_check_every
        self.ids_per_shard = ids_per_shard
        self.fsync_every = fsync_every
        self.backoff = backoff or Backoff()

        self.restart_idx = claim_restart(campaign_dir, task_id)
        self.tag = f"task{task_id:05d}_r{self.restart_idx:04d}"
        self.state_path = campaign_dir / "state" / f"task{task_id:05d}" / f"restart_{self.restart_idx:04d}.json"
        self.heartbeat_path = campaign_dir / "heartbeats" / f"task{task_id:05d}.json"
        self.shard = open(campaign_dir / "hits" / f"{self.tag}.jsonl", "a", buffering=1)

        self.stop = threading.Event()
        # Reentrant because the signal handler runs on the main thread and
        # may interrupt it while it holds the lock.
        self.lock = threading.RLock()
        self.next_idx = 0
        self.in_flight: set[int] = set()
        self.abandoned: list[int] = []
        self.queries_done = 0
        self.hits = 0
        self.errors: Counter = Counter()
        self.stop_reason: str | None = None
        self.started = time.monotonic()
        self.started_at = utcnow()
        self._recent: list[tuple[float, int]] = []
        self._ids_fh = None
        self._ids_part = -1
        self._ids_written = 0
        self._last_fsync = time.monotonic()

    def request_stop(self, reason: str) -> None:
        with self.lock:
            if self.stop_reason is None:
                self.stop_reason = reason
        self.stop.set()

    def _claim_idx(self) -> int | None:
        with self.lock:
            if self.stop.is_set():
                return None
            if self.max_queries is not None and self.next_idx >= self.max_queries:
                return None
            idx = self.next_idx
            self.next_idx += 1
            self.in_flight.add(idx)
            return idx

    def _write(self, rec: dict) -> None:
        self.shard.write(json.dumps(rec, separators=(",", ":")) + "\n")
        now = time.monotonic()
        if now - self._last_fsync > self.fsync_every:
            self.shard.flush()
            os.fsync(self.shard.fileno())
            self._last_fsync = now

    def _log_ids(self, idx: int, ids: list[str]) -> None:
        part = self._ids_written // self.ids_per_shard
        if part != self._ids_part:
            if self._ids_fh:
                self._ids_fh.close()
            self._ids_part = part
            self._ids_fh = gzip.open(self.dir / "ids" / f"{self.tag}_p{part:04d}.tsv.gz", "at")
        self._ids_fh.write(f"{idx}\t{','.join(ids)}\n")
        self._ids_written += 1

    def _on_error(self, kind: str) -> None:
        with self.lock:
            self.errors[kind] += 1

    def _thread_main(self, thread_no: int) -> None:
        backend = self.backend_factory()
        rng = random.Random(f"{self.campaign_id}-{self.task_id}-{self.restart_idx}-{thread_no}")
        while True:
            idx = self._claim_idx()
            if idx is None:
                return
            ids = query_ids(self.campaign_id, self.task_id, self.restart_idx, idx, self.n_ids)
            returned = search_with_retry(backend, build_query(ids, self.separator), self.backoff,
                                         self.stop, rng, on_error=self._on_error)
            with self.lock:
                if self.shard.closed:
                    return
                self.in_flight.discard(idx)
                if returned is None:
                    if not self.stop.is_set():
                        self.abandoned.append(idx)
                    continue
                self.queries_done += 1
                ts = iso(utcnow())
                for vid, queried, rank in match_hits(ids, returned):
                    self.hits += 1
                    self._write({"type": "hit", "video_id": vid, "queried_id": queried,
                                 "query_idx": idx, "queried_pos": ids.index(queried), "result_rank": rank, "n_results": len(returned),
                                 "campaign_id": self.campaign_id, "task_id": self.task_id,
                                 "restart_idx": self.restart_idx,
                                 "queries_done_at_hit": self.queries_done, "found_at": ts, "ts": ts})
                if self.log_ids:
                    self._log_ids(idx, ids)

    def snapshot(self, status: str) -> dict:
        now = time.monotonic()
        with self.lock:
            done, hits = self.queries_done, self.hits
            errors = dict(self.errors)
            in_flight = len(self.in_flight)
            abandoned = len(self.abandoned)
        self._recent.append((now, done))
        self._recent = [(t, d) for t, d in self._recent if now - t <= 300]
        t0, d0 = self._recent[0]
        elapsed = now - self.started
        return {
            "task_id": self.task_id, "restart_idx": self.restart_idx, "status": status,
            "host": socket.gethostname(), "pid": os.getpid(),
            "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
            "started_at": iso(self.started_at), "updated_at": iso(utcnow()),
            "threads": self.threads, "queries_done": done, "hits": hits,
            "qps_overall": done / elapsed if elapsed > 0 else 0.0,
            "qps_recent": (done - d0) / (now - t0) if now > t0 else 0.0,
            "errors": errors, "error_total": sum(errors.values()),
            "in_flight": in_flight, "abandoned": abandoned, "stop_reason": self.stop_reason,
        }

    def _heartbeat(self, status: str) -> dict:
        snap = self.snapshot(status)
        write_json_atomic(self.heartbeat_path, snap)
        with self.lock:
            self._write({"type": "progress", "queries_done": snap["queries_done"],
                         "hits": snap["hits"], "ts": snap["updated_at"]})
        return snap

    def _fleet_hits(self) -> int:
        total = 0
        for p in (self.dir / "heartbeats").glob("task*.json"):
            try:
                hb = json.loads(p.read_text())
                if hb.get("status") == "running":
                    total += hb.get("hits", 0)
            except (json.JSONDecodeError, OSError):
                continue
        # Heartbeats hold the current restart only; earlier restarts' hits
        # live in their final checkpoints.
        for p in (self.dir / "state").glob("task*/restart_*.json"):
            if p == self.state_path:
                continue
            try:
                st = json.loads(p.read_text())
            except (json.JSONDecodeError, OSError):
                continue
            if st.get("status") == "stopped":
                total += st.get("hits", 0)
        return total

    def _check_stop(self) -> None:
        if utcnow() >= self.deadline:
            self.request_stop("deadline")
        elif (self.dir / "STOP").exists():
            self.request_stop("stop_file")
        elif self._fleet_hits() >= int(self.manifest.get("target_unique", 1 << 62)):
            (self.dir / "STOP").write_text("target reached\n")
            self.request_stop("target")

    def run(self, join_timeout: float = 90.0) -> dict:
        threads = [threading.Thread(target=self._thread_main, args=(i,), daemon=True)
                   for i in range(self.threads)]
        for t in threads:
            t.start()
        last_hb = last_check = time.monotonic()
        self._heartbeat("running")
        while any(t.is_alive() for t in threads):
            self.stop.wait(1.0)
            now = time.monotonic()
            if now - last_check >= self.stop_check_every:
                self._check_stop()
                last_check = now
            if now - last_hb >= self.heartbeat_every:
                self._heartbeat("running")
                last_hb = now
            if self.stop.is_set():
                break
        if self.stop_reason is None and self.max_queries is not None:
            self.stop_reason = "max_queries"
        deadline = time.monotonic() + join_timeout
        for t in threads:
            t.join(max(0.0, deadline - time.monotonic()))
        return self.finish()

    def finish(self) -> dict:
        with self.lock:
            # Snapshot under the lock; a thread still stuck in a request after
            # the join timeout can no longer change what we record.
            self.stop.set()
            final = {
                "task_id": self.task_id, "restart_idx": self.restart_idx, "status": "stopped",
                "campaign_id": self.campaign_id, "stop_reason": self.stop_reason,
                "started_at": iso(self.started_at), "stopped_at": iso(utcnow()),
                "queries_done": self.queries_done, "hits": self.hits,
                "queries_issued": self.next_idx,
                "not_counted_query_idxs": sorted(self.in_flight) + sorted(self.abandoned),
                "errors": dict(self.errors), "log_ids": self.log_ids,
                "host": socket.gethostname(), "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
            }
            self._write({"type": "final", "queries_done": self.queries_done,
                         "hits": self.hits, "ts": final["stopped_at"]})
            self.shard.flush()
            os.fsync(self.shard.fileno())
            self.shard.close()
            if self._ids_fh:
                self._ids_fh.close()
        write_json_atomic(self.state_path, final)
        snap = self.snapshot("stopped")
        write_json_atomic(self.heartbeat_path, snap)
        return final


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m sampler.worker")
    p.add_argument("--campaign-dir", type=Path, required=True)
    p.add_argument("--task-id", type=int, default=int(os.environ.get("SLURM_ARRAY_TASK_ID", 0)))
    p.add_argument("--threads", type=int, default=int(os.environ.get("DIAL_THREADS", 8)))
    p.add_argument("--log-ids", action="store_true", help="write every queried ID to gzip shards")
    p.add_argument("--max-queries", type=int)
    p.add_argument("--base-url", help="search endpoint override (mock server)")
    p.add_argument("--heartbeat-every", type=float, default=30.0)
    args = p.parse_args(argv)

    factory = (lambda: InnertubeSearch(base_url=args.base_url)) if args.base_url else None
    w = Worker(args.campaign_dir, args.task_id, threads=args.threads, log_ids=args.log_ids,
               backend_factory=factory, max_queries=args.max_queries,
               heartbeat_every=args.heartbeat_every)
    for sig in (signal.SIGUSR1, signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda s, f: w.request_stop(signal.Signals(s).name))
    print(f"[{datetime.now():%F %T}] {w.tag} start deadline={w.deadline} threads={w.threads}", flush=True)
    final = w.run()
    print(f"[{datetime.now():%F %T}] {w.tag} stopped reason={final['stop_reason']} "
          f"queries={final['queries_done']} hits={final['hits']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
