#!/bin/zsh

# Get the least covered files across all non-test projects
# Usage: ./get-least-covered-files.sh [limit]
# Output: JSON array of files ordered by notCovered lines (descending)

set -e

# Get the script directory and repository root
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Get limit parameter (default: 5)
LIMIT="${1:-5}"

# Get project dependency order
PROJECTS_JSON=$("$SCRIPT_DIR/get-project-dependency-order.sh")

# Initialize results array
ALL_RESULTS=()

# Parse projects and process each non-test project
python3 <<PYTHON_SCRIPT
import sys
import json
import subprocess

projects_json = '''$PROJECTS_JSON'''
script_dir = "$SCRIPT_DIR"
limit = $LIMIT

# Parse projects
projects = json.loads(projects_json)

# Filter non-test projects
non_test_projects = [p for p in projects if p.get('isTestProject') is False]

# Collect all results
all_results = []

for project in non_test_projects:
    project_path = project['path']

    # Find the test project for this project
    # Convention: Project.Tests for Project
    test_project_name = project['name'] + '.Tests'
    test_project = next((p for p in projects if p['name'] == test_project_name), None)

    if not test_project:
        # No test project found, skip
        continue

    # Run coverage for the test project
    try:
        result = subprocess.run(
            [f"{script_dir}/run-project-coverage.sh", test_project['path'], str(limit)],
            capture_output=True,
            text=True,
            check=True
        )

        # Parse the JSON output
        coverage_data = json.loads(result.stdout)

        # Filter items with notCovered > 0
        for item in coverage_data:
            if item['lines']['notCovered'] > 0:
                all_results.append(item)

                # Check if we've hit the limit
                if len(all_results) >= limit:
                    break

        # Exit early if we've hit the limit
        if len(all_results) >= limit:
            break

    except subprocess.CalledProcessError:
        # Test project execution failed, skip
        continue
    except json.JSONDecodeError:
        # Invalid JSON output, skip
        continue

# Truncate to limit and output
all_results = all_results[:limit]
print(json.dumps(all_results, indent=2))

PYTHON_SCRIPT
