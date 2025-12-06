#!/bin/bash

# Get least covered files across all non-test projects
# Outputs JSON array of files with low coverage, sorted by notCovered (descending)
# Usage: ./get-least-covered-files.sh [limit]

set -e

# Disable MSBuild node reuse to prevent hanging processes
export MSBUILDDISABLENODEREUSE=1

# Navigate to project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT"

# Parse arguments
LIMIT=10
if [ $# -ge 1 ]; then
  LIMIT="$1"
fi

# Get projects in dependency order
PROJECTS_JSON=$("$SCRIPT_DIR/get-project-dependency-order.sh")

# Export variables for Python script
export SCRIPT_DIR
export LIMIT

# Process all projects and filter results
export PROJECTS_JSON
python3 << 'PYTHON_SCRIPT'
import json
import sys
import subprocess
import os
import re

# Get script directory and limit
script_dir = os.environ.get('SCRIPT_DIR', '')
limit = int(os.environ.get('LIMIT', '10'))
projects_json = os.environ.get('PROJECTS_JSON', '[]')

# Load projects
projects = json.loads(projects_json)

# Filter non-test projects
non_test_projects = [p for p in projects if not p.get('isTestProject', False)]

# Collect coverage data from all projects
all_coverage_data = []

for project in non_test_projects:
    project_path = project['path']

    # Run coverage for this project (suppress errors - some projects may not have tests)
    try:
        result = subprocess.run(
            [f"{script_dir}/get-project-coverage.sh", project_path, "999999"],
            capture_output=True,
            text=True,
            timeout=120
        )

        if result.returncode == 0 and result.stdout.strip():
            coverage_output = json.loads(result.stdout)
            all_coverage_data.extend(coverage_output)
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, json.JSONDecodeError):
        # Skip projects that fail or timeout
        pass

# Filter function to detect compiler-generated classes
def is_compiler_generated(name, path):
    # Common compiler-generated patterns in C#
    patterns = [
        r'<>c__DisplayClass',  # Closure display classes
        r'<>c',                # Compiler-generated helper classes
        r'<\w+>d__\d+',        # Iterator state machines (e.g., <GetEnumerator>d__1)
        r'<\w+>g__\w+\|\d+_\d+',  # Local functions
        r'__EqualityContract',  # Record equality contracts
        r'__StaticArrayInitTypeSize',  # Array initialization helpers
        r'\$\$method',         # Various compiler helpers
        r'PrivateImplementationDetails',  # Static initialization
    ]

    for pattern in patterns:
        if re.search(pattern, name):
            return True

    return False

# Filter out items with notCovered = 0
filtered_data = [
    item for item in all_coverage_data
    if item.get('lines', {}).get('notCovered', 0) > 0
]

# Filter out compiler-generated classes
filtered_data = [
    item for item in filtered_data
    if not is_compiler_generated(item.get('name', ''), item.get('path', ''))
]

# Sort by notCovered descending
sorted_data = sorted(
    filtered_data,
    key=lambda x: x.get('lines', {}).get('notCovered', 0),
    reverse=True
)

# Limit results
limited_data = sorted_data[:limit]

# Output in the same format as get-project-coverage.sh
print(json.dumps(limited_data))

PYTHON_SCRIPT
