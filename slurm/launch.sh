#!/bin/bash
# Create a campaign, size it from limits/limits.json, and submit the search
# array, the enrich job, and the merge job (after enrich finishes).
#
#   bash slurm/launch.sh NAME [--max-concurrent N] [--threads T] [--target N] [--days D]
#                             [--log-ids] [--prior PATH] [--partition P]
set -euo pipefail
NAME="${1:?campaign name}"; shift
MAXC=""; THREADS=8; TARGET=10016; DAYS=7; LOGIDS=0; PRIOR=""; PART=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --max-concurrent) MAXC="$2"; shift 2;;
        --threads) THREADS="$2"; shift 2;;
        --target) TARGET="$2"; shift 2;;
        --days) DAYS="$2"; shift 2;;
        --log-ids) LOGIDS=1; shift;;
        --prior) PRIOR="$2"; shift 2;;
        --partition) PART="$2"; shift 2;;
        *) echo "unknown option $1"; exit 2;;
    esac
done
export DIAL_REPO="$(cd "$(dirname "$0")/.." && pwd)"
source "$DIAL_REPO/slurm/env.sh"
mkdir -p logs
[[ -f limits/limits.json ]] || { echo "run: bash slurm/discover_limits.sh first"; exit 1; }

export CAMPAIGN_DIR="$DIAL_REPO/campaigns/$NAME"
SEP=$(python -c "import json;print(json.load(open('pilot/known_hit.json'))['chosen_separator'])" 2>/dev/null || echo OR)
python -m sampler.campaign create "$CAMPAIGN_DIR" --days "$DAYS" --target "$TARGET" --separator "$SEP" \
    ${PRIOR:+--prior-dataset "$PRIOR"}
python -m tools.size_campaign --limits limits/limits.json --cpus-per-task 2 --days "$DAYS" \
    ${MAXC:+--max-concurrent "$MAXC"} --out "$CAMPAIGN_DIR/plan.json"

read -r N TIME PLAN_PART < <(python -c "
import json; p=json.load(open('$CAMPAIGN_DIR/plan.json')); print(p['array_size'], p['time_limit'], p['partition'])")
PART="${PART:-$PLAN_PART}"
EXPORTS="ALL,DIAL_REPO=$DIAL_REPO,CAMPAIGN_DIR=$CAMPAIGN_DIR,DIAL_THREADS=$THREADS,DIAL_LOG_IDS=$LOGIDS"
COMMON="--partition=$PART --time=$TIME --export=$EXPORTS"
echo "$COMMON --cpus-per-task=2 --mem=2G" > "$CAMPAIGN_DIR/slurm_args"

ARR=$(sbatch --parsable $COMMON --cpus-per-task=2 --mem=2G --array="0-$((N - 1))%$N" slurm/campaign.sbatch)
ENR=$(sbatch --parsable $COMMON slurm/enrich.sbatch)
MRG=$(sbatch --parsable --partition="$PART" --export="$EXPORTS" --dependency=afterany:"$ENR" slurm/merge.sbatch)
echo "$ARR $ENR $MRG" > "$CAMPAIGN_DIR/job_ids"
echo "campaign $CAMPAIGN_DIR"
echo "search array $ARR (${N} workers x ${THREADS} threads, time $TIME, partition $PART)"
echo "enrich $ENR, merge $MRG (after enrich)"
echo "status: python -m tools.status $CAMPAIGN_DIR"
echo "raise/lower concurrency live: scontrol update JobId=$ARR ArrayTaskThrottle=K"
echo "stop cleanly: touch $CAMPAIGN_DIR/STOP"
