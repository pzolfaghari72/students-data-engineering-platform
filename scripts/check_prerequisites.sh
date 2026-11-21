#!/usr/bin/env bash

# Stop the script if an error occurs, an undefined variable is used,
# or a command in a pipeline fails.
set -Eeuo pipefail


# --------------------------------------------------
# Helper functions
# --------------------------------------------------

# Display an informational message.
log() {
    printf '[CHECK] %s\n' "$1"
}

# Display an error message and stop the script.
fail() {
    printf '[ERROR] %s\n' "$1" >&2
    exit 1
}


# --------------------------------------------------
# Project paths
# --------------------------------------------------

# Get the directory containing this script.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Get the project root directory (one level above scripts/).
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"


# --------------------------------------------------
# Select Python interpreter
# --------------------------------------------------

log "Selecting Python interpreter..."

# Use the Python interpreter in the following priority:
# 1. PROJECT_PYTHON environment variable
# 2. Project virtual environment (.venv)
# 3. System Python 3
# 4. System Python

if [[ -n "${PROJECT_PYTHON:-}" ]]; then
    PYTHON_BIN="$PROJECT_PYTHON"

elif [[ -x "$PROJECT_ROOT/.venv/bin/python" ]]; then
    PYTHON_BIN="$PROJECT_ROOT/.venv/bin/python"

elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"

elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python)"

else
    fail "Python is not installed. Please install Python 3.11 or 3.12."
fi

# Make sure the selected Python interpreter is executable.
[[ -x "$PYTHON_BIN" ]] \
    || fail "Python interpreter is not executable: $PYTHON_BIN"

log "Selected Python: $PYTHON_BIN"


# --------------------------------------------------
# Operating system
# --------------------------------------------------

log "Checking operating system..."

# Read operating system information from /etc/os-release.
if [[ ! -f /etc/os-release ]]; then
    fail "Unable to identify the operating system."
fi

source /etc/os-release

log "Operating system: ${PRETTY_NAME:-Unknown}"


# --------------------------------------------------
# Python version
# --------------------------------------------------

log "Checking Python version..."

# The project requires Python 3.11 or 3.12.
"$PYTHON_BIN" -c '
import sys

version = sys.version_info[:2]

if not ((3, 11) <= version < (3, 13)):
    raise SystemExit(
        f"Unsupported Python version: {sys.version.split()[0]}. "
        "Python 3.11 or 3.12 is required."
    )

print(f"Python version: {sys.version.split()[0]}")
' || fail "Python version check failed."


# --------------------------------------------------
# Docker CLI
# --------------------------------------------------

log "Checking Docker..."

# Make sure Docker CLI is installed.
if ! command -v docker >/dev/null 2>&1; then
    fail "Docker CLI is not installed."
fi

docker --version


# --------------------------------------------------
# Docker Compose
# --------------------------------------------------

log "Checking Docker Compose..."

# Make sure Docker Compose V2 is available.
if ! docker compose version >/dev/null 2>&1; then
    fail "Docker Compose plugin is not available."
fi

docker compose version


# --------------------------------------------------
# Docker daemon
# --------------------------------------------------

log "Checking Docker daemon..."

# Check whether Docker can communicate with its daemon.
if ! docker info >/dev/null 2>&1; then
    fail "Docker daemon is not running or is inaccessible."
fi

log "Docker daemon is running."


# --------------------------------------------------
# Final result
# --------------------------------------------------

printf '\n'
log "All prerequisites are satisfied."
log "Your development environment is ready."
