#!/bin/bash

# Find a GitHub issue with status "Todo" and update it to "In Progress"
# Returns the issue number

set -e

# Navigate to project root
cd "$(dirname "$0")/.."

# Get project ID
PROJECT_ID="PVT_kwDODEiIac4BIN8m"

# Find issues with Todo status using gh api
TODO_ISSUES=$(gh api graphql -f query='
{
  node(id: "'$PROJECT_ID'") {
    ... on ProjectV2 {
      items(last: 100) {
        nodes {
          id
          content {
            ... on Issue {
              number
              title
            }
          }
          fieldValues(first: 10) {
            nodes {
              ... on ProjectV2ItemFieldSingleSelectValue {
                name
                field {
                  ... on ProjectV2SingleSelectField {
                    name
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
' --jq '.data.node.items.nodes[] | select(.fieldValues.nodes[] | select(.field.name == "Status" and .name == "Todo")) | {itemId: .id, issueNumber: .content.number}' 2>/dev/null || echo "{}")

# Extract first issue
ITEM_ID=$(echo "$TODO_ISSUES" | head -n 1 | jq -r '.itemId')
ISSUE_NUM=$(echo "$TODO_ISSUES" | head -n 1 | jq -r '.issueNumber')

if [ -z "$ITEM_ID" ] || [ "$ITEM_ID" == "null" ]; then
  echo "No issues found with status 'Todo'"
  exit 1
fi

# Get the Status field ID
FIELD_ID=$(gh api graphql -f query='
{
  node(id: "'$PROJECT_ID'") {
    ... on ProjectV2 {
      fields(first: 20) {
        nodes {
          ... on ProjectV2SingleSelectField {
            id
            name
            options {
              id
              name
            }
          }
        }
      }
    }
  }
}
' --jq '.data.node.fields.nodes[] | select(.name == "Status") | {fieldId: .id, inProgressId: (.options[] | select(.name == "In Progress") | .id)}')

STATUS_FIELD_ID=$(echo "$FIELD_ID" | jq -r '.fieldId')
IN_PROGRESS_OPTION_ID=$(echo "$FIELD_ID" | jq -r '.inProgressId')

# Update status to "In Progress"
gh api graphql -f query='
mutation {
  updateProjectV2ItemFieldValue(
    input: {
      projectId: "'$PROJECT_ID'"
      itemId: "'$ITEM_ID'"
      fieldId: "'$STATUS_FIELD_ID'"
      value: {
        singleSelectOptionId: "'$IN_PROGRESS_OPTION_ID'"
      }
    }
  ) {
    projectV2Item {
      id
    }
  }
}
' > /dev/null 2>&1 || echo "Warning: Could not update status" >&2

echo "Issue #$ISSUE_NUM"
