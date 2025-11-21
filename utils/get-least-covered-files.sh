#!/bin/bash

# Get least covered files across all non-test projects
# Usage: ./get-least-covered-files.sh [limit]
# Output: JSON array of files with coverage info, sorted by notCovered descending

set -e

LIMIT=${1:-10}

# Navigate to project root
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# Pass script dir and limit to Python script
python3 - "$LIMIT" "$SCRIPT_DIR" "$PROJECT_ROOT" <<'PYTHON_SCRIPT'
import sys
import json
import subprocess
import re
import os

limit = int(sys.argv[1])
script_dir = sys.argv[2]
project_root = sys.argv[3]

# Get project dependency order
result = subprocess.run(
    [os.path.join(script_dir, 'get-project-dependency-order.sh')],
    capture_output=True,
    text=True,
    check=True,
    cwd=project_root
)
projects = json.loads(result.stdout)

# Filter out test projects
non_test_projects = [p for p in projects if not p.get('isTestProject', False)]

all_results = []

# Process each non-test project
for project in non_test_projects:
    project_path = project['path']

    try:
        # Run coverage for this project (no limit on individual project results)
        result = subprocess.run(
            [os.path.join(script_dir, 'get-project-coverage.sh'), project_path, '999999'],
            capture_output=True,
            text=True,
            check=True,
            cwd=project_root
        )

        coverage_data = json.loads(result.stdout)

        # Filter results
        for item in coverage_data:
            not_covered = item['lines']['notCovered']
            covered = item['lines']['covered']

            # Skip items with 0 notCovered
            if not_covered == 0:
                continue

            # Skip compiler-generated classes
            # Patterns: contains <>, starts with <, contains anonymous types like <>c, <>9, etc.
            class_name = item['name']
            if '<' in class_name or '>' in class_name:
                continue
            if class_name.startswith('__'):
                continue
            if re.search(r'<>.*__', class_name):
                continue
            if 'DisplayClass' in class_name:
                continue
            if class_name.endswith('__c'):
                continue

            # Skip files with ≥80% coverage
            total_lines = covered + not_covered
            if total_lines > 0:
                coverage_ratio = covered / total_lines
                if coverage_ratio >= 0.8:
                    continue

            all_results.append(item)

    except subprocess.CalledProcessError:
        # If coverage fails for a project, skip it
        continue
    except json.JSONDecodeError:
        # If output is not valid JSON, skip it
        continue

    # Stop if we have enough results
    if len(all_results) >= limit:
        break

# Sort by notCovered descending
all_results.sort(key=lambda x: x['lines']['notCovered'], reverse=True)

# Truncate to limit
all_results = all_results[:limit]

# Output in the same format as get-project-coverage.sh
print(json.dumps(all_results, indent=2))
PYTHON_SCRIPT
