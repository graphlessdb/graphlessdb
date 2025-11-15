#!/bin/bash

# Script to find a GitHub issue with status "Todo" and update it to "In Progress"
# Usage: ./begin-issue.sh

set -e

# Get the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Project and field IDs
PROJECT_ID="PVT_kwDODEiIac4BIN8m"
STATUS_FIELD_ID="PVTSSF_lADODEiIac4BIN8mzg4vkfc"
IN_PROGRESS_OPTION_ID="47fc9ee4"

echo "Searching for issues with status 'Todo'..."

# Get project items with status "Todo" from project #1
PROJECT_ITEMS=$(gh project item-list 1 --owner graphlessdb --format json --limit 100)

# Extract the first item with status "Todo" and get its issue number
ITEM_DATA=$(echo "$PROJECT_ITEMS" | jq -r '.items[] | select(.status == "Todo") | {number: .content.number, id: .id} | @json' | head -n 1)

# Check if we found an item
if [ -z "$ITEM_DATA" ]; then
    echo "No issues found with status 'Todo'"
    exit 1
fi

# Extract issue number and item ID
ISSUE_NUMBER=$(echo "$ITEM_DATA" | jq -r '.number')
ITEM_ID=$(echo "$ITEM_DATA" | jq -r '.id')

echo "Found issue #$ISSUE_NUMBER with status 'Todo'"

# Update the project item status to "In Progress"
echo "Updating issue #$ISSUE_NUMBER to 'In Progress'..."

gh project item-edit --project-id "$PROJECT_ID" --id "$ITEM_ID" --field-id "$STATUS_FIELD_ID" --single-select-option-id "$IN_PROGRESS_OPTION_ID"

if [ $? -eq 0 ]; then
    echo "✓ Successfully updated issue #$ISSUE_NUMBER to 'In Progress'"
    echo "$ISSUE_NUMBER"
else
    echo "✗ Failed to update issue #$ISSUE_NUMBER"
    exit 1
fi
