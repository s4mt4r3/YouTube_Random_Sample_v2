# Dialing for Videos on Amarel

A random sample of public YouTube videos using the method of McGrady, Zheng, Curran, Baumgartner & Zuckerman (2023), *Dialing for Videos*, JQD:DM 3. It replaces the earlier prefix-sampling pipeline (`../YouTube-Random-Sample`, 2,003 videos) for the main sample.

**Method.** Each query ORs together 32 random IDs. Positions 1–10 are letters only, so YouTube's case-insensitive search covers 2^10 case variants of each ID. Position 11 comes from `AEIMQUYcgkosw048`. A hit is a returned video whose ID equals a queried ID case-insensitively. For each hit, yt-dlp saves the metadata, transcripts, and audio. The size estimate is `2^64 / (queries × 32 × 1024 / hits)`.

Read `PILOT_REPORT.md` first. **YouTube search uses only the first 31 OR'ed IDs**, so the campaign reports both the paper-method estimate (32) and a cap-corrected one (31).

## Layout

| Path | What |
|---|---|
| `sampler/idgen.py` | ID generator, query builder, hit validation. Each query is regenerable from `SeedSequence([campaign_id, task_id, restart_idx], spawn_key=(query_idx,))` |
| `sampler/search.py` | InnerTube search (`innertube` package, unauthenticated) with retry, exponential backoff, and full jitter |
| `sampler/worker.py` | Long-running search worker: threads, append-only hit shards, heartbeat, checkpoint on USR1/TERM |
| `sampler/campaign.py` | Campaign manifest, atomic `restart_idx` claims, shard reading |
| `sampler/estimate.py` | Size estimate, CI, Table 1 year distribution, projection |
| `sampler/pilot.py` | Live self-tests and `PILOT_REPORT.md` |
| `enrich/worker.py` | Hit processing (separate job; the hit shards are its queue) |
| `tools/` | `amarel_limits` (discover limits), `size_campaign`, `status`, `merge`, `regen_ids` |
| `slurm/` | `discover_limits.sh`, `selftest.sbatch`, `launch.sh`, `campaign.sbatch`, `enrich.sbatch`, `merge.sbatch`, `env.sh` |
| `tests/` | Unit and integration tests; `mock_server.py` is a fake InnerTube with a planted hit rate |

Campaign directory: `campaigns/<name>/{campaign.json, plan.json, hits/, state/, heartbeats/, ids/, enriched/, merged/}`.

## Run, in order (on Amarel)

Copy or clone the repo to `/scratch/$USER/YouTube-Random-Sample-v2`, then work from that directory. Commands are short single lines, because pasting long lines over SSH has split them before.

**1. Environment** (once; reuses the Miniforge from the earlier project):

```bash
source /scratch/$USER/miniforge3/etc/profile.d/conda.sh
conda create -y -n dial python=3.11
conda activate dial
pip install -e ".[dev]"
```

Optional: install `deno` (a single binary in `~/bin`). Recent yt-dlp warns without a JS runtime, and some formats may be missing. `ffmpeg` is needed only for `--fetch-video-segments`.

**2. Discover the cluster limits** (login node; runs a 3-minute `srun` to test compute-node internet access):

```bash
bash slurm/discover_limits.sh main
cat limits/limits.json
```

**3. Self-tests** (offline unit tests, then the live tests from a compute node, including the 15-minute throughput run):

```bash
sbatch --partition=main --export=ALL,DIAL_REPO=$PWD,KNOWN=/scratch/$USER/YouTube-Random-Sample/sample.db slurm/selftest.sbatch
```

This regenerates `PILOT_REPORT.md` with an Amarel throughput row next to the Mac row.

**4. Launch** (creates the campaign with deadline = now + 7 days, sizes the array from `limits.json`, and submits search, enrich, and merge):

```bash
bash slurm/launch.sh main7d --prior /scratch/$USER/YouTube-Random-Sample/sample.db
```

Options: `--max-concurrent N`, `--threads T` (default 8), `--target N` (default 10,016), `--days D`, `--log-ids`, `--partition P`.

**5. Monitor and adjust:**

```bash
python -m tools.status campaigns/main7d
scontrol update JobId=<array job id> ArrayTaskThrottle=<K>
touch campaigns/main7d/STOP
```

`status` reports fleet qps, error mix, and `hits_beyond_search_cap`. `ArrayTaskThrottle` changes the number of concurrent workers without a restart. Creating `STOP` makes every worker checkpoint and exit.

**6. Merge** runs automatically after enrich finishes. To run it by hand:

```bash
python -m tools.merge campaigns/main7d
```

This writes `merged/videos.parquet` (unique videos plus enrichment, with `in_prior_sample`), `merged/hits.parquet`, and `merged/ESTIMATE.md`.

## How it stays correct

- **Denominator.** A query counts only after its results are processed. Hits and progress records are written under the same lock. In-flight, abandoned, and persistently empty queries are excluded and listed in the checkpoint.
- **Restarts.** At walltime minus 120 s, Slurm sends USR1. The worker flushes and checkpoints, then the task requeues itself; if `scontrol requeue` is refused it resubmits a one-task job instead. Each start claims a new `restart_idx` with `O_EXCL`, so RNG streams never overlap. A worker that crashes within 10 minutes of starting is not resubmitted, so a broken environment cannot loop.
- **Errors.** Every failure is retried with exponential backoff and full jitter, on a longer scale for 403/429. Workers do not die. Nothing else limits the request rate. There are no proxies, IP rotation, cookies, or accounts.
- **Reproducibility.** `python -m tools.regen_ids campaigns/X --task 7 --restart 2` regenerates every ID a restart searched. `--log-ids` also writes them to gzip shards.
- **Enrichment.** Bot-check errors are classified as `blocked` before any "unavailable" pattern is tried, and they are retried later. This fixes the earlier misclassification. `ok` and `unavailable` are terminal.

## Tests

```bash
pytest -q
python -m tests.mock_server --port 8765 --hit-den 4096
```

The test suite uses only the mock server, never YouTube. The second command runs the mock standalone for manual runs; point a worker at it with `--base-url http://127.0.0.1:8765/youtubei/v1/`.

## Scope

As in the paper, this collects public, searchable videos only, with no attempt at unlisted or private ones. Hand-coding and VoxLingua language detection are out of scope. Shorts vs long-form is recorded as raw signals only (`/shorts/<id>` status and Location without following redirects, duration, width/height), with no classification rule.
