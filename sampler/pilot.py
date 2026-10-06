"""Live self-tests, written to pilot/<test>.json and assembled into PILOT_REPORT.md.

  known-hit    1 known ID (random case) + 31 random IDs, 50 reps, for each separator
  max-ids      grow IDs per query until the known ID stops coming back
  throughput   single-node worker run against real search (default 15 min)
  report       assemble PILOT_REPORT.md, including the 7-day projection

python -m sampler.pilot known-hit --known ../ytsample_results/sample.db
"""

from __future__ import annotations

import argparse
import json
import platform
import random
import socket
import sqlite3
import sys
import threading
import time
from collections import Counter
from pathlib import Path

from .campaign import create_campaign, shard_totals
from .estimate import PAPER, projection
from .idgen import SEPARATORS, build_query, generate_ids, match_hits, query_rng
from .search import Backoff, InnertubeSearch, search_with_retry
from .worker import Worker

PILOT_DIR = Path("pilot")


def load_known(path: str) -> list[str]:
    p = Path(path)
    if p.suffix == ".db":
        with sqlite3.connect(p) as con:
            ids = [r[0] for r in con.execute("SELECT video_id FROM videos")]
    else:
        ids = [l.strip() for l in p.read_text().splitlines() if l.strip()]
    # A leading '-' would read as a search exclusion operator.
    return [i for i in ids if len(i) == 11 and i[0] not in "-_"]


def vary_case(vid: str, rng: random.Random) -> str:
    return "".join(c.swapcase() if c.isalpha() and rng.random() < 0.5 else c for c in vid)


def _query_with_known(known: str, n: int, rng: random.Random, seed: int, position: int | None = None):
    fillers = generate_ids(query_rng(seed, 0, 0, rng.randrange(1 << 30)), n - 1)
    varied = vary_case(known, rng)
    pos = rng.randrange(n) if position is None else position
    ids = fillers[:pos] + [varied] + fillers[pos:]
    return ids, varied, pos


def _search(backend, query: str, errors: Counter):
    return search_with_retry(backend, query, Backoff(max_attempts=5), threading.Event(), random.Random(),
                             on_error=lambda k: errors.update([k]))


def known_hit(known_ids: list[str], reps: int, seed: int) -> dict:
    rng = random.Random(seed)
    backend = InnertubeSearch()
    out = {"reps": reps, "seed": seed, "separators": {}}
    for sep in SEPARATORS:
        found, errors, trials, latencies = 0, Counter(), [], []
        for r in range(reps):
            known = rng.choice(known_ids)
            ids, varied, pos = _query_with_known(known, 32, rng, seed + r)
            q = build_query(ids, sep)
            t0 = time.time()
            res = _search(backend, q, errors)
            latencies.append(time.time() - t0)
            hit_ids = [h[0] for h in match_hits(ids, res or [])]
            ok = known in hit_ids
            found += ok
            trials.append({"known": known, "sent_as": varied, "position": pos, "found": ok,
                           "n_results": None if res is None else len(res), "other_hits": [h for h in hit_ids if h != known]})
        out["separators"][sep] = {"found": found, "reps": reps, "rate": found / reps, "errors": dict(errors),
                                  "mean_latency_s": sum(latencies) / len(latencies), "trials": trials}
        print(f"known-hit sep={sep!r}: {found}/{reps}", flush=True)
    # OR is the paper's operator; switch only if the other is clearly better,
    # not on a few trials of noise.
    rates = {s: r["rate"] for s, r in out["separators"].items()}
    out["chosen_separator"] = "OR" if rates["OR"] >= max(rates.values()) - 0.10 else max(rates, key=rates.get)
    return out


def max_ids(known_ids: list[str], sizes: list[int], trials: int, separator: str, seed: int,
            position: str = "last") -> dict:
    rng = random.Random(seed)
    backend = InnertubeSearch()
    rows, fails_in_row, errors = [], 0, Counter()
    for n in sizes:
        found = 0
        details = []
        for t in range(trials):
            known = rng.choice(known_ids)
            # Last position is the worst case if long queries are truncated.
            pos = n - 1 if position == "last" else None
            ids, varied, p = _query_with_known(known, n, rng, seed + n * 100 + t, pos)
            q = build_query(ids, separator)
            res = _search(backend, q, errors)
            ok = known in [h[0] for h in match_hits(ids, res or [])]
            found += ok
            details.append({"position": p, "found": ok, "query_chars": len(q),
                            "n_results": None if res is None else len(res)})
        rows.append({"ids": n, "found": found, "trials": trials, "query_chars": details[0]["query_chars"],
                     "details": details})
        print(f"max-ids n={n}: {found}/{trials} (chars={details[0]['query_chars']})", flush=True)
        # Baseline recall is ~90%, so only a clear collapse ends the scan.
        fails_in_row = fails_in_row + 1 if found / trials < 0.5 else 0
        if fails_in_row >= 2:
            break
    base = rows[0]["found"] / rows[0]["trials"] if rows else 0
    workable = [r["ids"] for r in rows if r["found"] / r["trials"] >= base - 0.2]
    return {"separator": separator, "trials_per_size": trials, "position": position, "rows": rows,
            "errors": dict(errors), "baseline_rate_at_first_size": base,
            "largest_workable": max(workable) if workable else None}


def throughput(minutes: float, threads: int, separator: str, label: str) -> dict:
    cdir = PILOT_DIR / "throughput_campaign" / f"{label}_{int(time.time())}"
    create_campaign(cdir, days=1, target=10**9, separator=separator)
    w = Worker(cdir, 0, threads=threads, heartbeat_every=30)
    timer = threading.Timer(minutes * 60, w.request_stop, args=("pilot_time_limit",))
    timer.start()
    t0 = time.time()
    final = w.run()
    timer.cancel()
    elapsed = time.time() - t0
    totals = shard_totals(cdir)
    return {"label": label, "host": socket.gethostname(), "platform": platform.platform(),
            "minutes": minutes, "threads": threads, "elapsed_s": elapsed, "separator": separator,
            "queries_done": final["queries_done"], "hits": final["hits"],
            "hit_ids": [h["video_id"] for h in totals["hits"]],
            "qps": final["queries_done"] / elapsed, "qps_per_thread": final["queries_done"] / elapsed / threads,
            "errors": final["errors"], "abandoned": len(final["not_counted_query_idxs"]),
            "campaign_dir": str(cdir)}


def _fmt(x, f="{:,.0f}"):
    return "n/a" if x is None else f.format(x)


def report(pilot_dir: Path) -> str:
    def load(name):
        p = pilot_dir / f"{name}.json"
        return json.loads(p.read_text()) if p.exists() else None

    kh, mx = load("known_hit"), load("max_ids")
    tps = sorted(pilot_dir.glob("throughput_*.json"))
    L = ["# PILOT_REPORT", "",
         "Self-tests for the Dialing for Videos campaign (McGrady et al. 2023). "
         "Generated by `python -m sampler.pilot report`.", ""]
    L += ["## Summary of findings", "",
          "- **OR syntax and case-insensitivity work** (test 1). `OR` and `|` are equivalent; `OR` is kept as the paper's operator.",
          "- **YouTube searches only the first 31 OR'ed IDs.** The 32nd and later are ignored, independent of query length "
          "(test 2: last-position known ID found at 16/24/30/31 IDs, never at 32/33/40). A 32-ID query therefore covers "
          "31 x 1024 IDs, not 32,768. The campaign still sends 32 IDs as published, records each hit's position "
          "(`queried_pos`; a hit at position 31 would mean the cap changed), and reports two estimates: the paper method "
          "(32) and a cap-corrected one (31), which is 32/31 = 3.2% higher. Whether the cap existed in 2022 cannot be "
          "checked from here.",
          "- **Known-hit recall: 90/100 at random positions.** Of the 10 misses, 3 were at position 31 (the cap above), "
          "5 were known IDs containing `_` or `--` (the old prefix sample's IDs all contain a dash; campaign IDs are "
          "letters only), and 2 are unexplained. In the position scan, IDs at positions 22-30 were found 53/54 times (both separators).",
          "- **Empty result pages are a throttling signal.** In the local throughput run they were 25% of attempts, alongside "
          "HTTP 403s, at only 7 qps from one home IP. Empty pages are retried, and a query still empty after retries is "
          "excluded from the denominator rather than counted as zero hits.",
          "- A bug found and fixed while running test 1: the hit validator required position 11 to match case-exactly, which "
          "rejected ~half of the known IDs (sent with flipped case). Earlier runs are kept as "
          "`pilot/known_hit_run*_*.json` for the record.", ""]
    L += ["## 1. Known-hit test (OR syntax + case-insensitivity)", ""]
    if kh:
        L += ["One ID from the earlier 2,003-video sample, sent with randomly flipped case at a random position, "
              f"plus 31 random letter IDs; {kh['reps']} repetitions per separator.", "",
              "| Separator | Found | Rate | Errors | Mean latency |", "|---|---|---|---|---|"]
        for sep, r in kh["separators"].items():
            L.append(f"| `{sep}` | {r['found']}/{r['reps']} | {r['rate']:.0%} | {r['errors'] or 'none'} | {r['mean_latency_s']:.2f} s |")
        misses = [t for r in kh["separators"].values() for t in r["trials"] if not t["found"]]
        L += ["", f"Chosen separator: **`{kh['chosen_separator']}`**. Misses: {len(misses)}."]
        if misses:
            L += ["", "Missed trials (known ID, as sent, position, results returned):", ""]
            L += [f"- `{t['known']}` sent as `{t['sent_as']}` at {t['position']}, {t['n_results']} results" for t in misses[:20]]
    else:
        L.append("Not run.")
    L += ["", "## 2. Max IDs per query", ""]
    if mx:
        L += [f"Separator `{mx['separator']}`, {mx['trials_per_size']} trials per size, known ID placed "
              f"{'last (worst case if long queries are truncated)' if mx.get('position') == 'last' else 'at random'}.", "",
              "| IDs/query | Query chars | Found |", "|---|---|---|"]
        L += [f"| {r['ids']} | {r['query_chars']} | {r['found']}/{r['trials']} |" for r in mx["rows"]]
        L += ["", f"Largest size within 20 points of the 32-ID rate: **{mx['largest_workable']}**. "
              "The campaign stays at 32 to match the paper."]
    else:
        L.append("Not run.")
    L += ["", "## 3. Throughput", ""]
    tp_rows = [json.loads(p.read_text()) for p in tps]
    if tp_rows:
        L += ["| Run | Host | Minutes | Threads | Queries | qps | qps/thread | Hits | Errors | Abandoned |",
              "|---|---|---|---|---|---|---|---|---|---|"]
        for t in tp_rows:
            L.append(f"| {t['label']} | {t['host']} | {t['elapsed_s'] / 60:.1f} | {t['threads']} | {t['queries_done']:,} | "
                     f"{t['qps']:.2f} | {t['qps_per_thread']:.2f} | {t['hits']} | {t['errors'] or 'none'} | {t['abandoned']} |")
        L += ["", "Report only; the campaign is not gated on these numbers."]
    else:
        L.append("Not run.")
    L += ["", "## 4. Mock backend", "",
          "`tests/mock_server.py` serves the InnerTube search endpoint with a planted hit rate "
          "(an ID exists iff `sha1(id.lower()) % hit_den == 0`, returned in a hash-derived case), decoy results, "
          "and injected 429/500 errors. The unit and integration tests (`pytest -q`) run only against it.", ""]
    L += ["## 5. Projection (7 days)", "",
          f"Paper rate: {PAPER['case_insensitive_ids']:,} case-insensitive IDs / 32 / {PAPER['hits']:,} hits = "
          f"**{PAPER['queries_per_hit']:,.0f} queries per hit**. Paper's own average pace: "
          f"{PAPER['total_queries'] / (69 * 86400):.0f} qps over 69 days.", "",
          "| Basis | Fleet qps | Queries in 7 days | Expected hits (paper rate) |", "|---|---|---|---|"]
    week = 7 * 86400
    for t in tp_rows:
        p = projection(t["qps"], week)
        L.append(f"| {t['label']}, 1 node ({t['threads']} threads) | {t['qps']:.1f} | {p['queries']:,.0f} | {p['expected_hits']:,.0f} |")
    if tp_rows:
        per_thread = max(t["qps_per_thread"] for t in tp_rows)
        for workers, threads in ((50, 8), (200, 8), (500, 8)):
            q = per_thread * workers * threads
            p = projection(q, week)
            L.append(f"| {workers} workers x {threads} threads at {per_thread:.2f} qps/thread, if it scales | "
                     f"{q:,.0f} | {p['queries']:,.0f} | {p['expected_hits']:,.0f} |")
    for q in (96, 500, 1000):
        p = projection(q, week)
        L.append(f"| reference: {q} qps | {q} | {p['queries']:,.0f} | {p['expected_hits']:,.0f} |")
    L += ["", "Fleet rows assume per-thread throughput holds as workers are added from Amarel's shared egress. "
          "The earlier prefix-sampling run saw HTTP 403 on 27% of single-threaded queries from Amarel, so "
          "treat them as upper bounds until the Amarel throughput run (`slurm/selftest.sbatch`) and the first "
          "hours of the campaign show the real fleet qps. If YouTube has grown since 2022, the hit rate per "
          "query rises in proportion, so the paper rate is conservative on that axis."]
    return "\n".join(L) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m sampler.pilot")
    sub = ap.add_subparsers(dest="cmd", required=True)
    k = sub.add_parser("known-hit")
    k.add_argument("--known", required=True, help="old sample.db or a file of video IDs")
    k.add_argument("--reps", type=int, default=50)
    k.add_argument("--seed", type=int, default=20261006)
    m = sub.add_parser("max-ids")
    m.add_argument("--known", required=True)
    m.add_argument("--sizes", default="32,33,34,35,36,38,40,44,48,64")
    m.add_argument("--trials", type=int, default=10)
    m.add_argument("--position", choices=["last", "random"], default="last")
    m.add_argument("--separator", choices=list(SEPARATORS))
    m.add_argument("--seed", type=int, default=20261007)
    t = sub.add_parser("throughput")
    t.add_argument("--minutes", type=float, default=15)
    t.add_argument("--threads", type=int, default=8)
    t.add_argument("--separator", choices=list(SEPARATORS))
    t.add_argument("--label", default=socket.gethostname().split(".")[0])
    sub.add_parser("report")
    a = ap.parse_args(argv)
    PILOT_DIR.mkdir(exist_ok=True)

    def chosen_sep(given):
        if given:
            return given
        p = PILOT_DIR / "known_hit.json"
        return json.loads(p.read_text())["chosen_separator"] if p.exists() else "OR"

    if a.cmd == "known-hit":
        res = known_hit(load_known(a.known), a.reps, a.seed)
        (PILOT_DIR / "known_hit.json").write_text(json.dumps(res, indent=1))
    elif a.cmd == "max-ids":
        res = max_ids(load_known(a.known), [int(x) for x in a.sizes.split(",")], a.trials,
                      chosen_sep(a.separator), a.seed, a.position)
        (PILOT_DIR / "max_ids.json").write_text(json.dumps(res, indent=1))
    elif a.cmd == "throughput":
        res = throughput(a.minutes, a.threads, chosen_sep(a.separator), a.label)
        (PILOT_DIR / f"throughput_{a.label}.json").write_text(json.dumps(res, indent=1))
        print(json.dumps({k: v for k, v in res.items() if k != "hit_ids"}, indent=1))
    Path("PILOT_REPORT.md").write_text(report(PILOT_DIR))
    return 0


if __name__ == "__main__":
    sys.exit(main())
