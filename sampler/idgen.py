"""Candidate ID generation for Dialing for Videos (McGrady et al. 2023).

Positions 1-10 are letters only, so each search covers 2**10 case variants
per ID (YouTube search is case-insensitive). Position 11 comes from the 16
characters YouTube actually uses there.

Every query is regenerable from (campaign_id, task_id, restart_idx,
query_idx) alone: the query index is the SeedSequence spawn key, which gives
statistically independent child streams without replaying earlier queries.
"""

from __future__ import annotations

import string

import numpy as np

LETTERS = string.ascii_uppercase + string.ascii_lowercase
ALPHABET64 = LETTERS + string.digits + "-_"
POS11 = "AEIMQUYcgkosw048"
IDS_PER_QUERY = 32
CASE_FACTOR = 2**10
ID_SPACE = 64**10 * 16
assert ID_SPACE == 2**64

SEPARATORS = {"OR": " OR ", "|": " | "}

_LETTERS_ARR = np.array(list(LETTERS))
_POS11_ARR = np.array(list(POS11))


def query_rng(campaign_id: int, task_id: int, restart_idx: int, query_idx: int) -> np.random.Generator:
    ss = np.random.SeedSequence([campaign_id, task_id, restart_idx], spawn_key=(query_idx,))
    return np.random.Generator(np.random.PCG64(ss))


def generate_ids(rng: np.random.Generator, n: int = IDS_PER_QUERY) -> list[str]:
    """Draw n IDs that are distinct case-insensitively.

    Two IDs differing only in case are the same search target, so a
    case-only duplicate would double-count coverage in the estimator.
    """
    out: list[str] = []
    seen: set[str] = set()
    while len(out) < n:
        need = n - len(out)
        body = _LETTERS_ARR[rng.integers(0, len(LETTERS), size=(need, 10))]
        last = _POS11_ARR[rng.integers(0, len(POS11), size=need)]
        for row, tail in zip(body, last):
            vid = "".join(row) + tail
            key = vid.lower()
            if key not in seen:
                seen.add(key)
                out.append(vid)
    return out


def query_ids(campaign_id: int, task_id: int, restart_idx: int, query_idx: int,
              n: int = IDS_PER_QUERY) -> list[str]:
    return generate_ids(query_rng(campaign_id, task_id, restart_idx, query_idx), n)


def build_query(ids: list[str], separator: str = "OR") -> str:
    return SEPARATORS[separator].join(ids)


def is_valid_candidate(vid: str) -> bool:
    return len(vid) == 11 and all(c in LETTERS for c in vid[:10]) and vid[10] in POS11


def match_hits(queried: list[str], returned: list[str]) -> list[tuple[str, str, int]]:
    """Return (video_id, queried_id, rank) for returned videos that are hits.

    A hit must equal a queried ID case-insensitively; everything else search
    returns (filler and related videos) is discarded. Position 11 needs no
    separate case check: each valid position-11 letter's case twin is not a
    valid position-11 character, so lowercase equality is already exact.
    """
    by_key = {q.lower(): q for q in queried}
    hits = []
    seen: set[str] = set()
    for rank, vid in enumerate(returned):
        if not isinstance(vid, str) or len(vid) != 11 or vid in seen:
            continue
        q = by_key.get(vid.lower())
        if q is not None:
            seen.add(vid)
            hits.append((vid, q, rank))
    return hits
