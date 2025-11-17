#!/bin/bash

# Get least covered files across all non-test projects
# Usage: ./get-least-covered-files.sh [limit]

set -e

LIMIT=${1:-5}

# Navigate to project root
cd "$(dirname "$0")/.."

# Get project list using get-project-dependency-order.sh
PROJECTS_JSON=$("$(dirname "$0")/get-project-dependency-order.sh")

# Save to temp file for Python script
TEMP_PROJECTS=$(mktemp)
echo "$PROJECTS_JSON" > "$TEMP_PROJECTS"

# Parse projects and run coverage for non-test projects
python3 - "$TEMP_PROJECTS" "$LIMIT" <<'PYTHON_SCRIPT'
import sys
import json
import subprocess
import os

projects_file = sys.argv[1]
limit = int(sys.argv[2])

with open(projects_file, 'r') as f:
    projects_json = f.read()

projects = json.loads(projects_json)

all_results = []

for project in projects:
    # Only process test projects (they test the non-test projects)
    if not project['isTestProject']:
        continue

    project_path = project['path']

    # Run coverage for this test project
    try:
        result = subprocess.run(
            ['./utils/run-project-coverage.sh', project_path, '1000'],
            capture_output=True,
            text=True,
            timeout=300
        )

        if result.returncode == 0:
            files = json.loads(result.stdout)

            # Filter files
            for file_info in files:
                not_covered = file_info['lines']['notCovered']
                covered = file_info['lines']['covered']
                total = covered + not_covered

                # Skip if no uncovered lines
                if not_covered == 0:
                    continue

                # Skip if coverage ratio >= 0.8 (80%)
                if total > 0 and (covered / total) >= 0.8:
                    continue

                # Skip compiler-generated files
                path = file_info['path']
                if '/obj/' in path or '\\obj\\' in path:
                    continue
                if '.g.cs' in path or 'Generated' in path or '.Designer.' in path:
                    continue

                all_results.append(file_info)

                # Early exit if we have enough results
                if len(all_results) >= limit:
                    break

        # Early exit if we have enough results
        if len(all_results) >= limit:
            break

    except Exception as e:
        # Continue to next project on error
        continue

# Sort by notCovered descending
all_results.sort(key=lambda x: x['lines']['notCovered'], reverse=True)

# Limit results
all_results = all_results[:limit]

print(json.dumps(all_results, indent=2))
PYTHON_SCRIPT

# Clean up temp file
rm -f "$TEMP_PROJECTS"
