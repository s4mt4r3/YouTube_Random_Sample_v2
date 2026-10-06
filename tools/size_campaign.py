"""Size the campaign array from discovered limits (limits/limits.json).

Workers are long-running and requeue themselves until the deadline, so
array size = number of concurrent workers. Each limit that applies is
listed and the smallest wins; a few job slots stay reserved for the
enrich and merge jobs.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def size(limits: dict, cpus_per_task: int, reserve_jobs: int = 4, max_concurrent: int | None = None,
         days: float = 7.0) -> dict:
    caps = {}
    if limits.get("max_array_size"):
        caps["MaxArraySize (indices < MaxArraySize)"] = limits["max_array_size"] - 1
    for key in ("qos_max_jobs_pu", "assoc_max_jobs"):
        if limits.get(key):
            caps[key] = limits[key] - reserve_jobs
    for key in ("qos_max_submit_pu", "assoc_max_submit"):
        if limits.get(key):
            caps[key] = limits[key] - reserve_jobs
    for key in ("qos_max_cpus_pu", "assoc_grp_cpus", "partition_max_cpus_per_user"):
        if limits.get(key):
            caps[f"{key} / cpus_per_task"] = limits[key] // cpus_per_task - reserve_jobs
    if limits.get("partition_total_cpus"):
        caps["partition_total_cpus / cpus_per_task"] = limits["partition_total_cpus"] // cpus_per_task
    if max_concurrent:
        caps["--max-concurrent (user choice)"] = max_concurrent
    if not caps:
        raise SystemExit("no limits found in limits.json; rerun tools/amarel_limits.py on Amarel")
    binding = min(caps, key=caps.get)
    n = max(1, caps[binding])
    walls = [w for w in (limits.get("partition_max_time_min"), limits.get("qos_max_wall_min")) if w]
    wall = min(walls) if walls else 72 * 60
    return {
        "array_size": n,
        "array_spec": f"0-{n - 1}",
        "binding_limit": binding,
        "caps": caps,
        "cpus_per_task": cpus_per_task,
        "time_limit_min": wall,
        "time_limit": f"{wall // 1440}-{(wall % 1440) // 60:02d}:{wall % 60:02d}:00",
        "restarts_per_task_for_campaign": -(-int(days * 1440) // wall),
        "partition": limits.get("partition"),
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limits", type=Path, default=Path("limits/limits.json"))
    ap.add_argument("--cpus-per-task", type=int, default=2)
    ap.add_argument("--max-concurrent", type=int)
    ap.add_argument("--days", type=float, default=7.0)
    ap.add_argument("--out", type=Path)
    a = ap.parse_args(argv)
    plan = size(json.loads(a.limits.read_text()), a.cpus_per_task, max_concurrent=a.max_concurrent, days=a.days)
    text = json.dumps(plan, indent=1)
    if a.out:
        a.out.write_text(text)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
