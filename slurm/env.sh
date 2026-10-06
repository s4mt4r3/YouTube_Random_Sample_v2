# Sourced by every sbatch script. Amarel's system Python is 3.9 and its
# modules stop at 3.8, so the env lives in a user Miniforge on /scratch.
: "${DIAL_REPO:?DIAL_REPO must be exported (launch.sh does this)}"
CONDA_BASE="${DIAL_CONDA_BASE:-/scratch/$USER/miniforge3}"
source "$CONDA_BASE/etc/profile.d/conda.sh"
conda activate "${DIAL_ENV:-dial}"
cd "$DIAL_REPO"
export PYTHONUNBUFFERED=1
