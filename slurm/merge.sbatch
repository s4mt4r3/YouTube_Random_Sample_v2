#!/bin/bash
#SBATCH --job-name=dial-merge
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#SBATCH --time=02:00:00
#SBATCH --output=logs/%x_%j.log
set -euo pipefail
source "$DIAL_REPO/slurm/env.sh"
python -m tools.merge "$CAMPAIGN_DIR"
python -m tools.status "$CAMPAIGN_DIR" | tee "$CAMPAIGN_DIR/merged/final_status.txt"
