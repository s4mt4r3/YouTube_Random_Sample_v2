"""Regenerate the exact IDs searched by any worker restart.

python -m tools.regen_ids campaigns/<name> --task 7 --restart 2 [--query 1234] [--out ids.tsv.gz]

Without --query, writes every counted query of that restart (queries_issued
from the final checkpoint, minus not_counted_query_idxs).
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from pathlib import Path

from sampler.campaign import load_manifest
from sampler.idgen import build_query, query_ids


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("campaign_dir", type=Path)
    ap.add_argument("--task", type=int, required=True)
    ap.add_argument("--restart", type=int, required=True)
    ap.add_argument("--query", type=int)
    ap.add_argument("--out", type=Path)
    a = ap.parse_args(argv)
    m = load_manifest(a.campaign_dir)
    cid, n = int(m["campaign_id"]), int(m.get("ids_per_query", 32))
    if a.query is not None:
        ids = query_ids(cid, a.task, a.restart, a.query, n)
        print(build_query(ids, m["separator"]))
        return 0
    st = json.loads((a.campaign_dir / "state" / f"task{a.task:05d}" / f"restart_{a.restart:04d}.json").read_text())
    if st.get("status") != "stopped":
        sys.exit("restart has no final checkpoint (hard kill); use --query with indices from its hit shard")
    skip = set(st["not_counted_query_idxs"])
    fh = gzip.open(a.out, "wt") if a.out else sys.stdout
    for q in range(st["queries_issued"]):
        if q not in skip:
            fh.write(f"{q}\t{','.join(query_ids(cid, a.task, a.restart, q, n))}\n")
    if a.out:
        fh.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
