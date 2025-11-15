#!/bin/bash

# Script to create GitHub issues for files with least test coverage
# Usage: ./create-unit-test-issues.sh [limit]

set -e

# Get the script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Default limit is 10
LIMIT="${1:-10}"

echo "Getting least covered files with limit: $LIMIT"

# Run get-least-covered-files.sh to get the files
COVERAGE_DATA=$("$SCRIPT_DIR/get-least-covered-files.sh" "$LIMIT")

# Check if we got any data
if [ -z "$COVERAGE_DATA" ] || [ "$COVERAGE_DATA" = "[]" ]; then
    echo "No files found that need coverage improvement"
    exit 0
fi

# Parse the JSON array and create issues for each item
echo "$COVERAGE_DATA" | jq -c '.[]' | while read -r item; do
    # Extract the name property
    NAME=$(echo "$item" | jq -r '.name')

    # Use the entire JSON object as the description
    DESCRIPTION="$item"

    # Create the issue title
    TITLE="Create unit tests for $NAME"

    echo "Creating issue: $TITLE"

    # Create the GitHub issue
    gh issue create \
        --title "$TITLE" \
        --body "$DESCRIPTION" \
        --repo graphlessdb/graphlessdb

    if [ $? -eq 0 ]; then
        echo "✓ Created issue for $NAME"
    else
        echo "✗ Failed to create issue for $NAME"
    fi
done

echo "Completed creating GitHub issues"
