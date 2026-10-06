#!/bin/bash
#SBATCH --job-name=dial-enrich
#SBATCH --requeue
#SBATCH --signal=B:USR1@120
#SBATCH --open-mode=append
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --output=logs/%x_%A_%a.log
# Hit processing, separate from search: reads the hit shards as a queue,
# runs until the deadline plus drain, requeues itself at walltime.
# Optional: DIAL_SEGMENTS=1 (needs ffmpeg) and DIAL_SEGMENTS_DIR.
set -uo pipefail
source "$DIAL_REPO/slurm/env.sh"
EXTRA=()
[[ "${DIAL_SEGMENTS:-0}" == 1 ]] && EXTRA+=(--fetch-video-segments)
python -m enrich.worker --campaign-dir "$CAMPAIGN_DIR" \
    --worker-idx "${SLURM_ARRAY_TASK_ID:-0}" --n-workers "${DIAL_ENRICH_WORKERS:-1}" \
    --threads "${DIAL_ENRICH_THREADS:-4}" ${EXTRA[@]+"${EXTRA[@]}"} &
PID=$!
on_signal() {
    kill -USR1 "$PID" 2>/dev/null
    wait "$PID"
    scontrol requeue "${SLURM_JOB_ID}" || true
    exit 0
}
trap on_signal USR1 TERM
wait "$PID"
