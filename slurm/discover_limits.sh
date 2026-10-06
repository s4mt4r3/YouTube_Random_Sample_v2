#!/bin/bash
# Run on an Amarel login node from the repo root:
#   bash slurm/discover_limits.sh [partition]
# Writes limits/limits.json and limits/limits_raw.txt; nothing is hardcoded.
set -euo pipefail
export DIAL_REPO="$(cd "$(dirname "$0")/.." && pwd)"
source "$DIAL_REPO/slurm/env.sh"
python -m tools.amarel_limits --out limits --partition "${1:-main}"
