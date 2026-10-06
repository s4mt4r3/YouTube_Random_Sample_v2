"""Merge hit shards + enrichment into Parquet, dedupe, and write the estimate.

Outputs (in <campaign>/merged/):
  videos.parquet   one row per unique video_id, with enrichment fields and
                   in_prior_sample (overlap with the earlier prefix sample)
  hits.parquet     every hit record (duplicates kept; the estimator uses all hits)
  estimate.json / ESTIMATE.md   size estimate and upload-year table

The prior dataset can be the old sample.db (videos table) or any
.csv/.parquet/.txt with a video_id column or one ID per line.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

import pandas as pd

from enrich.worker import status_by_id
from sampler.campaign import load_manifest, shard_totals
from sampler.estimate import both_estimates, format_report, year_distribution


def load_prior(path: str | None) -> set[str]:
    if not path:
        return set()
    p = Path(path)
    if p.suffix == ".db":
        with sqlite3.connect(p) as con:
            return {r[0] for r in con.execute("SELECT video_id FROM videos")}
    if p.suffix == ".parquet":
        return set(pd.read_parquet(p)["video_id"])
    if p.suffix == ".csv":
        return set(pd.read_csv(p)["video_id"])
    return {l.strip() for l in p.read_text().splitlines() if l.strip()}


def merge(campaign_dir: Path, prior: str | None = None) -> dict:
    m = load_manifest(campaign_dir)
    prior = prior or m.get("prior_dataset")
    out = campaign_dir / "merged"
    out.mkdir(exist_ok=True)
    totals = shard_totals(campaign_dir)
    hits = pd.DataFrame(totals["hits"])
    if hits.empty:
        hits = pd.DataFrame(columns=["video_id", "found_at", "queried_id", "query_idx", "task_id", "restart_idx"])
    hits.to_parquet(out / "hits.parquet", index=False)

    videos = hits.sort_values("found_at").drop_duplicates("video_id", keep="first").copy()
    videos["times_hit"] = videos["video_id"].map(hits["video_id"].value_counts())
    prior_ids = load_prior(prior)
    videos["in_prior_sample"] = videos["video_id"].isin(prior_ids)

    enriched = status_by_id(campaign_dir / "enriched")
    if enriched:
        e = pd.DataFrame(list(enriched.values()))
        for col in ("steps", "tags", "categories", "caption_langs", "auto_caption_langs", "segment_ranges"):
            if col in e:
                e[col] = e[col].map(lambda v: None if v is None or v != v else json.dumps(v))
        e = e.rename(columns={"status": "enrich_status", "id": "yt_id"})
        videos = videos.merge(e, on="video_id", how="left", suffixes=("", "_enrich"))
    videos.to_parquet(out / "videos.parquet", index=False)

    ests = both_estimates(totals["total_queries"], len(hits), m)
    est = ests["paper_method"]
    dates = videos["upload_date"].tolist() if "upload_date" in videos else []
    ok = videos[videos.get("enrich_status", pd.Series(dtype=str)) == "ok"] if "enrich_status" in videos else videos.iloc[0:0]
    years = year_distribution(ok["upload_date"].tolist(), est["estimated_public_videos"]) if len(ok) else []
    summary = {
        "estimate": est, "estimate_cap_corrected": ests["cap_corrected"],
        "ids_sent_per_query": ests["ids_sent_per_query"], "ids_searched_per_query": ests["ids_searched_per_query"],
        "years": years,
        "unique_videos": int(len(videos)), "hits": int(len(hits)),
        "duplicate_hits": int(len(hits) - len(videos)),
        "in_prior_sample": int(videos["in_prior_sample"].sum()),
        "prior_dataset": prior, "prior_size": len(prior_ids),
        "enriched_ok": int(len(ok)), "videos_without_upload_date": int(len(dates) - len(ok)),
        "shards": len(totals["shards"]), "unclean_shards": sum(not s["clean"] for s in totals["shards"]),
    }
    (out / "estimate.json").write_text(json.dumps(summary, indent=1, default=str))
    (out / "ESTIMATE.md").write_text(
        f"# Size estimate ({m['created_at']} to {m['deadline']})\n\n"
        f"## Paper method ({ests['ids_sent_per_query']} IDs per query)\n\n{format_report(est, years)}\n\n"
        f"## Cap-corrected ({ests['ids_searched_per_query']} IDs per query actually searched)\n\n"
        f"{format_report(ests['cap_corrected'])}\n\n"
        f"Unique videos {summary['unique_videos']:,}; duplicate hits {summary['duplicate_hits']}; "
        f"overlap with prior sample {summary['in_prior_sample']} of {summary['prior_size']:,}.\n"
        "Year shares use enriched videos only; the size estimate uses all hits.\n")
    return summary


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("campaign_dir", type=Path)
    ap.add_argument("--prior", help="old dataset (sample.db / csv / parquet / txt)")
    a = ap.parse_args(argv)
    s = merge(a.campaign_dir, a.prior)
    print(json.dumps({k: v for k, v in s.items() if k != "years"}, indent=1, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
