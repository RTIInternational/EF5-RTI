#!/bin/bash
#SBATCH --job-name=submit_all_basins
#SBATCH --account=gfdl_b
#SBATCH --time=15:00:00
#SBATCH --ntasks=1603
#SBATCH --cpus-per-task=1
#SBATCH --output=logs/%x_%j.out
#SBATCH --error=logs/%x_%j.err

set -euo pipefail

mkdir -p logs

# Replace this with your real script path when it is ready.
TARGET_SCRIPT="/path/to/your/future/script.sh"

RUNTIME_LOG="logs/${SLURM_JOB_NAME}_${SLURM_JOB_ID}_runtime.log"

log() {
	echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] $*" | tee -a "$RUNTIME_LOG"
}

on_error() {
	local exit_code=$?
	log "ERROR: job failed with exit code ${exit_code}"
	exit "$exit_code"
}
trap on_error ERR

log "Job started"
log "Job ID: ${SLURM_JOB_ID}"
log "Node list: ${SLURM_JOB_NODELIST:-N/A}"
log "Working directory: $(pwd)"
log "Requested tasks: ${SLURM_NTASKS:-N/A}"

if [[ ! -f "$TARGET_SCRIPT" ]]; then
	log "Placeholder script not found: $TARGET_SCRIPT"
	log "Update TARGET_SCRIPT in submit_all_basins.slurm and resubmit."
	exit 1
fi

if [[ ! -x "$TARGET_SCRIPT" ]]; then
	log "Script is not executable; applying chmod +x to: $TARGET_SCRIPT"
	chmod +x "$TARGET_SCRIPT"
fi

log "Launching script: $TARGET_SCRIPT"
srun "$TARGET_SCRIPT"

log "Job completed successfully"
