#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Get the root directory (parent of utils)
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Get project dependency order
projects_json=$("$SCRIPT_DIR/get-project-dependency-order.sh")

# Parse and iterate through projects that are NOT test projects
echo "$projects_json" | jq -c '.[]' | while read -r project; do
  is_test_project=$(echo "$project" | jq -r '.isTestProject')

  # Skip test projects - only process non-test projects
  if [ "$is_test_project" = "true" ]; then
    continue
  fi

  project_name=$(echo "$project" | jq -r '.name')

  # Find the corresponding test project
  test_project_path=$(echo "$projects_json" | jq -r --arg name "${project_name}.Tests" '.[] | select(.name == $name) | .path')

  # Skip if no test project found
  if [ -z "$test_project_path" ]; then
    continue
  fi

  # Run coverage for the test project with limit of 1 to get the least covered file
  # Suppress stderr to avoid error messages in output
  coverage_json=$("$SCRIPT_DIR/run-project-coverage.sh" "$test_project_path" 1 2>/dev/null)

  # Check if we got valid JSON output
  if echo "$coverage_json" | jq -e . >/dev/null 2>&1; then
    # Check if there are any results
    array_length=$(echo "$coverage_json" | jq 'length')
    if [ "$array_length" -gt 0 ]; then
      # Get the first item's notCovered value
      not_covered=$(echo "$coverage_json" | jq -r '.[0].lines.notCovered')

      # If notCovered is non-zero, output the result and exit
      if [ -n "$not_covered" ] && [ "$not_covered" -gt 0 ]; then
        file_path=$(echo "$coverage_json" | jq -r '.[0].path')
        echo "{\"filePath\":\"$file_path\"}"
        exit 0
      fi
    fi
  fi
done