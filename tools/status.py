"""Campaign status: queries, hits, hits/hour, size estimate, unique videos,
download backlog, error rate, and Shorts-signal counts.

python -m tools.status campaigns/<name> [--json]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

from enrich.worker import status_by_id
from sampler.campaign import load_manifest, read_jsonl, shard_totals, utcnow
from sampler.estimate import PAPER, both_estimates


def collect(campaign_dir: Path) -> dict:
    m = load_manifest(campaign_dir)
    totals = shard_totals(campaign_dir)
    hits = totals["hits"]
    unique = {h["video_id"] for h in hits}
    ests = both_estimates(totals["total_queries"], len(hits), m)
    est = ests["paper_method"]
    cap = ests["ids_searched_per_query"]
    beyond_cap = sum(1 for h in hits if h.get("queried_pos", 0) >= cap)

    hbs = []
    for p in sorted((campaign_dir / "heartbeats").glob("task*.json")):
        try:
            hbs.append(json.loads(p.read_text()))
        except json.JSONDecodeError:
            continue
    now = utcnow()
    running = [h for h in hbs if h.get("status") == "running"
               and (now - datetime.fromisoformat(h["updated_at"])).total_seconds() < 300]
    stale = [h["task_id"] for h in hbs if h.get("status") == "running" and h not in running]
    errors = Counter()
    for h in hbs:
        errors.update(h.get("errors", {}))
    for p in (campaign_dir / "state").glob("task*/restart_*.json"):
        try:
            st = json.loads(p.read_text())
        except json.JSONDecodeError:
            continue
        if st.get("status") == "stopped":
            errors.update(st.get("errors", {}))
    # Heartbeats of running tasks also cover their current restart only, so
    # the error total spans current + finished restarts without overlap,
    # except a stopped task's last heartbeat; good enough for a rate.
    attempts = totals["total_queries"] + sum(errors.values())

    created = datetime.fromisoformat(m["created_at"])
    hours = max((now - created).total_seconds() / 3600, 1e-9)
    found_times = sorted(h["found_at"] for h in hits)
    last24 = sum(1 for t in found_times if (now - datetime.fromisoformat(t)).total_seconds() < 86400)

    enriched = status_by_id(campaign_dir / "enriched")
    estatus = Counter(r["status"] for r in enriched.values())
    shorts = Counter()
    for r in enriched.values():
        if r["status"] != "ok":
            continue
        shorts[f"shorts_probe_{r.get('shorts_status')}"] += 1
        d, a = r.get("duration"), r.get("aspect_w_over_h")
        if d is not None:
            shorts["duration<=60s" if d <= 60 else ("60s<duration<=180s" if d <= 180 else "duration>180s")] += 1
        if a:
            shorts["vertical(w/h<1)" if a < 1 else ("square" if a == 1 else "horizontal")] += 1
    backlog = len(unique) - estatus.get("ok", 0) - estatus.get("unavailable", 0)

    return {
        "campaign_dir": str(campaign_dir), "now": now.isoformat(timespec="seconds"),
        "deadline": m["deadline"], "target_unique": m.get("target_unique"),
        "stop_file": (campaign_dir / "STOP").exists(),
        "workers_running": len(running), "workers_stale": stale,
        "fleet_qps_recent": sum(h.get("qps_recent", 0) for h in running),
        "total_queries": totals["total_queries"], "hits": len(hits), "unique_videos": len(unique),
        "duplicate_hits": len(hits) - len(unique),
        "hits_per_hour_overall": len(hits) / hours, "hits_last_24h": last24,
        "queries_per_hit": totals["total_queries"] / len(hits) if hits else None,
        "paper_queries_per_hit": PAPER["queries_per_hit"],
        "estimate": est, "estimate_cap_corrected": ests["cap_corrected"],
        "ids_sent_per_query": ests["ids_sent_per_query"], "ids_searched_per_query": cap,
        "hits_beyond_search_cap": beyond_cap,
        "errors": dict(errors), "error_rate": sum(errors.values()) / attempts if attempts else 0.0,
        "enrich_status": dict(estatus), "download_backlog": backlog,
        "shorts_signals": dict(shorts),
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("campaign_dir", type=Path)
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    s = collect(a.campaign_dir)
    if a.json:
        print(json.dumps(s, indent=1, default=str))
        return 0
    e = s["estimate"]
    fmt = lambda x: "n/a" if x is None else f"{x:,.0f}"
    print(f"campaign      {s['campaign_dir']}  deadline {s['deadline']}  STOP={s['stop_file']}")
    print(f"workers       running={s['workers_running']} stale={len(s['workers_stale'])}  fleet qps(5m)={s['fleet_qps_recent']:.1f}")
    print(f"queries       {s['total_queries']:,}   error rate {s['error_rate']:.2%}  {s['errors']}")
    print(f"hits          {s['hits']:,} (unique {s['unique_videos']:,}, dup {s['duplicate_hits']})  "
          f"{s['hits_per_hour_overall']:.2f}/h overall, {s['hits_last_24h']} in last 24h  target {s['target_unique']}")
    print(f"queries/hit   {fmt(s['queries_per_hit'])} (paper {s['paper_queries_per_hit']:,.0f})")
    c = s["estimate_cap_corrected"]
    print(f"size estimate {fmt(e['estimated_public_videos'])} paper method, {s['ids_sent_per_query']} IDs/query "
          f"(95% CI {fmt(e['ci95_low'])} to {fmt(e['ci95_high'])})")
    print(f"              {fmt(c['estimated_public_videos'])} cap-corrected, {s['ids_searched_per_query']} IDs/query searched; "
          f"hits from beyond the cap: {s['hits_beyond_search_cap']} (should be 0)")
    print(f"enrich        {s['enrich_status']}  backlog={s['download_backlog']}")
    print(f"shorts        {s['shorts_signals']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
