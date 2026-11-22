#!/usr/bin/env bash

set -Eeuo pipefail

# --------------------------------------------------
# Error handling
# --------------------------------------------------

log() {
    printf '\n[INFO] %s\n' "$1"
}

error() {
    printf '\n[ERROR] %s\n' "$1" >&2
}

fail() {
    error "$1"
    exit 1
}

trap 'error "Initialization failed at line $LINENO."' ERR
# --------------------------------------------------
# Project configuration
# --------------------------------------------------

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_SCRIPTS_DIR="$PROJECT_ROOT/scripts"

if [[ -x "$PROJECT_ROOT/.venv/bin/python" ]]; then
    export PROJECT_PYTHON="$PROJECT_ROOT/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
    export PROJECT_PYTHON="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
    export PROJECT_PYTHON="$(command -v python)"
else
    fail "Python 3.11 or 3.12 is required."
fi

cd "$PROJECT_ROOT"

# --------------------------------------------------
# Step 1: Environment setup
# --------------------------------------------------

log "Preparing environment configuration..."

if [[ ! -f ".env" ]]; then

    [[ -f ".env.example" ]] \
        || fail ".env.example was not found."

    cp .env.example .env

    log ".env file created from .env.example."

else
    log ".env already exists. Existing configuration preserved."
fi

chmod 600 .env

# --------------------------------------------------
# Step 2: Prerequisite checks
# --------------------------------------------------

log "Checking system prerequisites..."

bash "$PROJECT_SCRIPTS_DIR/check_prerequisites.sh"

# --------------------------------------------------
# Step 3: Secret generation
# --------------------------------------------------

log "Generating missing security secrets..."

"$PROJECT_PYTHON" "$PROJECT_SCRIPTS_DIR/generate_secrets.py"

# --------------------------------------------------
# Step 4: Environment validation
# --------------------------------------------------

log "Validating environment configuration..."

"$PROJECT_PYTHON" "$PROJECT_SCRIPTS_DIR/validate_environment.py" --mode core

# --------------------------------------------------
# Step 5: Project directories
# --------------------------------------------------

log "Preparing project directories..."

bash "$PROJECT_SCRIPTS_DIR/create_directories.sh"

# --------------------------------------------------
# Step 6: Docker Compose validation
# --------------------------------------------------

log "Validating Docker Compose configuration..."

docker compose --profile cdc --profile streaming config --quiet

log "Docker Compose configuration is valid."

# --------------------------------------------------
# Completion summary
# --------------------------------------------------

printf '\n'
printf '%s\n' "=========================================="
printf '%s\n' " PROJECT INITIALIZATION COMPLETED"
printf '%s\n' "=========================================="
printf '\n'

printf '%s\n' "Environment configuration : Ready"
printf '%s\n' "Security secrets          : Checked"
printf '%s\n' "Prerequisites             : Passed"
printf '%s\n' "Project directories       : Ready"
printf '%s\n' "Docker Compose            : Validated"

printf '\n'
printf '%s\n' "Next steps:"
printf '%s\n' "  make up"
printf '%s\n' "  make status"
printf '\n'