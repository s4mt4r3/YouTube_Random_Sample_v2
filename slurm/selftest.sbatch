#!/bin/bash
#SBATCH --job-name=dial-selftest
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=01:00:00
#SBATCH --output=logs/%x_%j.log
# sbatch --partition=main --export=ALL,DIAL_REPO=$PWD,KNOWN=/path/to/sample.db slurm/selftest.sbatch
# Unit tests (mock backend only), then the live self-tests from a compute node.
set -uo pipefail
source "$DIAL_REPO/slurm/env.sh"
python -m pytest -q || { echo "unit tests failed"; exit 1; }
python -m sampler.pilot known-hit --known "$KNOWN"
python -m sampler.pilot max-ids --known "$KNOWN" --sizes 16,24,30,31,32,33,40 --trials 8
python -m sampler.pilot throughput --minutes "${MINUTES:-15}" --threads "${THREADS:-8}" --label "amarel-$(hostname -s)"
echo "wrote PILOT_REPORT.md"
