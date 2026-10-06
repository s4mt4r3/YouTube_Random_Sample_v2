#!/bin/bash
#SBATCH --job-name=dial-campaign
#SBATCH --requeue
#SBATCH --signal=B:USR1@120
#SBATCH --open-mode=append
#SBATCH --output=logs/%x_%A_%a.log
# Submitted by slurm/launch.sh (partition, time, array, cpus come from the
# discovered limits). One long-running worker per array task. At walltime
# minus 120 s Slurm sends USR1 to this shell; the trap forwards it, the
# worker flushes its shard and checkpoint, and the task requeues itself
# until the campaign deadline. Every restart claims a new restart_idx.
set -uo pipefail
source "$DIAL_REPO/slurm/env.sh"
TASK="${SLURM_ARRAY_TASK_ID:?}"
EXTRA=()
[[ "${DIAL_LOG_IDS:-0}" == 1 ]] && EXTRA+=(--log-ids)

if ! python -m sampler.campaign before-deadline "$CAMPAIGN_DIR"; then
    echo "task $TASK: campaign past deadline or STOP present; not starting"
    exit 0
fi

python -m sampler.worker --campaign-dir "$CAMPAIGN_DIR" --task-id "$TASK" \
    --threads "${DIAL_THREADS:-8}" ${DIAL_BASE_URL:+--base-url "$DIAL_BASE_URL"} ${EXTRA[@]+"${EXTRA[@]}"} &
PID=$!
STARTED=$(date +%s)

resubmit() {
    if python -m sampler.campaign before-deadline "$CAMPAIGN_DIR"; then
        if ! scontrol requeue "${SLURM_ARRAY_JOB_ID}_${TASK}"; then
            # Fallback chain if requeue is refused: a fresh one-task job.
            sbatch $(cat "$CAMPAIGN_DIR/slurm_args") --array="$TASK" "$DIAL_REPO/slurm/campaign.sbatch"
        fi
    fi
}

on_signal() {
    echo "task $TASK: $1 received at $(date '+%F %T'); checkpointing"
    kill -USR1 "$PID" 2>/dev/null
    wait "$PID"
    resubmit
    exit 0
}
trap 'on_signal USR1' USR1
trap 'on_signal TERM' TERM

wait "$PID"
RC=$?
# The worker exits by itself only at the deadline, on STOP/target, or on a
# crash. A crash after a real run is resubmitted; a crash within 10 minutes
# of starting (bad env, import error) is not, so it cannot loop forever.
if [[ $RC -ne 0 ]]; then
    if (( $(date +%s) - STARTED > 600 )); then
        echo "task $TASK: worker exited rc=$RC after a real run; resubmitting"
        resubmit
        exit 0
    fi
    echo "task $TASK: worker failed rc=$RC within 10 min of start; not resubmitting"
    exit "$RC"
fi
exit 0
