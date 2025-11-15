#!/bin/zsh

# Save solution coverage to coverage.json at the root folder
# Usage: ./save-solution-coverage.sh

set -e

# Get the script directory and repository root
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Call get-solution-coverage.sh and capture the output
COVERAGE_JSON=$("$SCRIPT_DIR/get-solution-coverage.sh")

# Write the result to coverage.json at the root folder
echo "$COVERAGE_JSON" > "$REPO_ROOT/coverage.json"

echo "Coverage saved to $REPO_ROOT/coverage.json"
