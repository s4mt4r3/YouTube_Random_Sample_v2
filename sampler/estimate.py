"""Size estimate and upload-year distribution, as in McGrady et al. (2023).

guesses_per_hit = total_queries * ids_per_query * 2**10 / hits
estimated_public_videos = 2**64 / guesses_per_hit

The paper's numbers: 18,260,259,669 case-insensitive IDs, 10,016 hits,
1,866,863,608 guesses per hit, 9,881,141,822 videos (late 2022). Table 1
gives the share of sampled videos per upload year; multiplying the shares by
the size estimate gives estimated uploads per year (their Figure 2), and the
running sum gives the estimated total size by year (their Figure 1).
"""

from __future__ import annotations

import math
from collections import Counter

from .idgen import CASE_FACTOR, ID_SPACE, IDS_PER_QUERY

PAPER = {
    "case_insensitive_ids": 18_260_259_669,
    "case_sensitive_guesses": 18_698_505_901_056,
    "hits": 10_016,
    "guesses_per_hit": 1_866_863_608,
    "estimated_videos": 9_881_141_822,
    "window": "2022-10-05 to 2022-12-13",
}
PAPER["total_queries"] = PAPER["case_insensitive_ids"] / IDS_PER_QUERY
PAPER["queries_per_hit"] = PAPER["total_queries"] / PAPER["hits"]

PAPER_TABLE1 = {
    2005: 0.00, 2006: 0.05, 2007: 0.22, 2008: 0.43, 2009: 0.74, 2010: 1.13,
    2011: 1.67, 2012: 1.86, 2013: 1.97, 2014: 2.34, 2015: 3.02, 2016: 4.25,
    2017: 5.39, 2018: 6.73, 2019: 8.81, 2020: 15.22, 2021: 20.29, 2022: 25.91,
}


def size_estimate(total_queries: float, hits: int, ids_per_query: int = IDS_PER_QUERY) -> dict:
    guesses = total_queries * ids_per_query * CASE_FACTOR
    out = {
        "total_queries": total_queries,
        "hits": hits,
        "case_sensitive_guesses": guesses,
        "guesses_per_hit": None,
        "estimated_public_videos": None,
        "ci95_low": None,
        "ci95_high": None,
        "margin_of_error": None,
    }
    if hits <= 0 or total_queries <= 0:
        return out
    gph = guesses / hits
    n_hat = ID_SPACE / gph
    # Hits are a Poisson count over a fixed number of guesses, so the
    # relative standard error is 1/sqrt(hits); at the paper's 10,016 hits
    # this is the ~2% margin of error they report.
    moe = 1.96 / math.sqrt(hits)
    out.update(guesses_per_hit=gph, estimated_public_videos=n_hat,
               ci95_low=n_hat * (1 - moe), ci95_high=n_hat * (1 + moe), margin_of_error=moe)
    return out


def year_distribution(upload_dates: list[str | None], n_hat: float | None) -> list[dict]:
    """upload_dates are yt-dlp 'YYYYMMDD' strings; missing dates are reported, not dropped."""
    years = Counter()
    for d in upload_dates:
        years[int(d[:4]) if d and len(d) >= 4 and d[:4].isdigit() else None] += 1
    known = sum(v for k, v in years.items() if k is not None)
    rows, cumulative = [], 0.0
    for year in sorted(k for k in years if k is not None):
        share = years[year] / known
        uploads = share * n_hat if n_hat else None
        cumulative += uploads or 0.0
        rows.append({"year": year, "count": years[year], "pct": 100 * share,
                     "paper_pct": PAPER_TABLE1.get(year),
                     "est_uploads": uploads, "est_cumulative": cumulative if n_hat else None})
    if years.get(None):
        rows.append({"year": None, "count": years[None], "pct": None, "paper_pct": None,
                     "est_uploads": None, "est_cumulative": None})
    return rows


def projection(qps: float, seconds: float, queries_per_hit: float = PAPER["queries_per_hit"]) -> dict:
    queries = qps * seconds
    return {"qps": qps, "seconds": seconds, "queries": queries,
            "queries_per_hit": queries_per_hit, "expected_hits": queries / queries_per_hit}


def format_report(est: dict, years: list[dict] | None = None) -> str:
    def n(x, fmt="{:,.0f}"):
        return "n/a" if x is None else fmt.format(x)

    lines = [
        f"- Total queries: {n(est['total_queries'])}",
        f"- Hits: {n(est['hits'])}",
        f"- Case-sensitive guesses: {n(est['case_sensitive_guesses'])}",
        f"- Guesses per hit: {n(est['guesses_per_hit'])} (paper: {PAPER['guesses_per_hit']:,})",
        f"- Estimated public, searchable videos: {n(est['estimated_public_videos'])} "
        f"(95% CI {n(est['ci95_low'])} to {n(est['ci95_high'])}; paper: {PAPER['estimated_videos']:,})",
    ]
    if years:
        lines += ["", "| Year | Count | % of sample | Paper Table 1 % | Est. uploads | Est. cumulative size |",
                  "|---|---|---|---|---|---|"]
        for r in years:
            lines.append(f"| {r['year'] or 'unknown'} | {r['count']} | {n(r['pct'], '{:.2f}')} | "
                         f"{n(r['paper_pct'], '{:.2f}')} | {n(r['est_uploads'])} | {n(r['est_cumulative'])} |")
    return "\n".join(lines)


def both_estimates(total_queries: float, hits: int, manifest: dict) -> dict:
    """Paper-method estimate (all IDs sent) and cap-corrected (IDs actually searched)."""
    sent = int(manifest.get("ids_per_query", IDS_PER_QUERY))
    searched = int(manifest.get("searched_ids_per_query", sent))
    return {"paper_method": size_estimate(total_queries, hits, sent),
            "cap_corrected": size_estimate(total_queries, hits, searched),
            "ids_sent_per_query": sent, "ids_searched_per_query": searched}
