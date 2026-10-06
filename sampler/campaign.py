"""Campaign manifest, restart claims, and shard reading.

Directory layout under a campaign dir:
  campaign.json                      manifest (campaign_id, deadline, target, ...)
  state/task00007/restart_0003.json  one claim + final checkpoint per restart
  hits/task00007_r0003.jsonl         append-only: hit and progress records
  heartbeats/task00007.json          latest worker status (overwritten atomically)
  ids/task00007_r0003_p0000.tsv.gz   all queried IDs (only with --log-ids)
  enriched/                          hit-processing outputs
  STOP                               touch to stop every worker cleanly
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


def write_json_atomic(path: Path, data: dict) -> None:
    tmp = path.with_suffix(path.suffix + f".tmp{os.getpid()}")
    tmp.write_text(json.dumps(data, indent=1, sort_keys=True))
    os.replace(tmp, path)


def create_campaign(campaign_dir: Path, days: float, target: int, separator: str,
                    ids_per_query: int = 32, campaign_id: int | None = None,
                    prior_dataset: str | None = None, searched_ids_per_query: int | None = None) -> dict:
    campaign_dir.mkdir(parents=True, exist_ok=True)
    path = campaign_dir / "campaign.json"
    if path.exists():
        raise SystemExit(f"{path} already exists; refusing to overwrite a campaign")
    now = utcnow()
    manifest = {
        "campaign_id": campaign_id if campaign_id is not None else secrets.randbits(63),
        "created_at": iso(now),
        "deadline": iso(now + timedelta(days=days)),
        "target_unique": target,
        "separator": separator,
        "ids_per_query": ids_per_query,
        # Measured 2026-10-06: search only uses the first 31 OR'ed terms, so
        # a 32-ID query really searches 31. Used for the cap-corrected estimate.
        "searched_ids_per_query": min(ids_per_query, searched_ids_per_query or ids_per_query),
        "prior_dataset": prior_dataset,
        "method": "McGrady et al. 2023 Dialing for Videos",
    }
    for sub in ("state", "hits", "heartbeats", "ids", "enriched"):
        (campaign_dir / sub).mkdir(exist_ok=True)
    write_json_atomic(path, manifest)
    return manifest


def load_manifest(campaign_dir: Path) -> dict:
    return json.loads((campaign_dir / "campaign.json").read_text())


def deadline_of(manifest: dict) -> datetime:
    return datetime.fromisoformat(manifest["deadline"])


def claim_restart(campaign_dir: Path, task_id: int) -> int:
    """Atomically claim the next unused restart_idx for a task.

    O_EXCL creation makes the claim safe even if a requeued task and a
    resubmitted copy of it start at the same time; each gets a distinct
    restart_idx and hence a non-overlapping RNG stream.
    """
    task_dir = campaign_dir / "state" / f"task{task_id:05d}"
    task_dir.mkdir(parents=True, exist_ok=True)
    existing = [int(p.stem.split("_")[1]) for p in task_dir.glob("restart_*.json")]
    idx = max(existing, default=-1) + 1
    while True:
        path = task_dir / f"restart_{idx:04d}.json"
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            idx += 1
            continue
        with os.fdopen(fd, "w") as fh:
            json.dump({"task_id": task_id, "restart_idx": idx, "status": "claimed",
                       "claimed_at": iso(utcnow()), "host": os.uname().nodename,
                       "slurm_job_id": os.environ.get("SLURM_JOB_ID")}, fh)
        return idx


def read_jsonl(path: Path):
    """Yield records, skipping a torn final line from a hard kill."""
    with open(path, "rb") as fh:
        for line in fh:
            if not line.endswith(b"\n"):
                break
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def shard_totals(campaign_dir: Path) -> dict:
    """Combine every shard into estimator inputs.

    A shard's query count is its final checkpoint if the worker stopped
    cleanly, otherwise the largest counter seen in its progress and hit
    records. Hits are counted only up to that same point, so the
    numerator and denominator always describe the same queries.
    """
    shards = []
    for path in sorted((campaign_dir / "hits").glob("task*_r*.jsonl")):
        task = int(path.stem[4:9])
        restart = int(path.stem.split("_r")[1])
        hits, queries, last_ts = [], 0, None
        for rec in read_jsonl(path):
            if rec.get("type") == "hit":
                hits.append(rec)
                queries = max(queries, rec["queries_done_at_hit"])
            elif rec.get("type") in ("progress", "final"):
                queries = max(queries, rec["queries_done"])
            last_ts = rec.get("ts", last_ts)
        state = campaign_dir / "state" / f"task{task:05d}" / f"restart_{restart:04d}.json"
        clean = False
        if state.exists():
            try:
                st = json.loads(state.read_text())
                if st.get("status") == "stopped":
                    queries = st["queries_done"]
                    clean = True
            except (json.JSONDecodeError, KeyError):
                pass
        shards.append({"task": task, "restart": restart, "queries": queries,
                       "hits": hits, "clean": clean, "last_ts": last_ts, "path": str(path)})
    return {
        "shards": shards,
        "total_queries": sum(s["queries"] for s in shards),
        "hits": [h for s in shards for h in s["hits"]],
    }


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m sampler.campaign")
    sub = p.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("create", help="write campaign.json (deadline = now + days)")
    c.add_argument("campaign_dir", type=Path)
    c.add_argument("--days", type=float, default=7.0)
    c.add_argument("--target", type=int, default=10_016)
    c.add_argument("--separator", choices=["OR", "|"], default="OR")
    c.add_argument("--ids-per-query", type=int, default=32, help="32 = the paper")
    c.add_argument("--searched-ids-per-query", type=int, default=31,
                   help="IDs YouTube actually searches per query (pilot: 31)")
    c.add_argument("--campaign-id", type=int)
    c.add_argument("--prior-dataset")
    b = sub.add_parser("before-deadline", help="exit 0 if the campaign should keep running")
    b.add_argument("campaign_dir", type=Path)
    b.add_argument("--margin-min", type=float, default=5.0)
    args = p.parse_args(argv)
    if args.cmd == "create":
        m = create_campaign(args.campaign_dir, args.days, args.target, args.separator,
                            ids_per_query=args.ids_per_query, campaign_id=args.campaign_id,
                            prior_dataset=args.prior_dataset,
                            searched_ids_per_query=args.searched_ids_per_query)
        print(json.dumps(m, indent=1))
        return 0
    m = load_manifest(args.campaign_dir)
    alive = (utcnow() + timedelta(minutes=args.margin_min) < deadline_of(m)
             and not (args.campaign_dir / "STOP").exists())
    return 0 if alive else 1


if __name__ == "__main__":
    sys.exit(main())
