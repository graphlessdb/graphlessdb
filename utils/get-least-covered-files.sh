#!/bin/zsh

# Find the least covered files across all non-test projects
# Usage: ./get-least-covered-files.sh [limit]
# Output: JSON array of files ordered by notCovered lines (descending)

set -e

# Get the script directory and repository root
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Parse arguments
LIMIT="${1:-5}"

# Get project dependency order
PROJECTS_JSON=$("$SCRIPT_DIR/get-project-dependency-order.sh")

# Parse projects and collect least covered files using a temporary Python script
TEMP_SCRIPT=$(mktemp)
cat > "$TEMP_SCRIPT" <<'EOF'
import sys
import json
import subprocess
import os

limit = int(sys.argv[1])
script_dir = sys.argv[2]

# Read projects from stdin
projects_json = sys.stdin.read()
projects = json.loads(projects_json)

# Collect all least covered files from non-test projects
all_files = []

for project in projects:
    # Skip test projects
    if project['isTestProject']:
        continue

    project_path = project['path']

    # Try to get coverage for this project
    # We need to find the corresponding test project
    project_name = project['name']
    test_project_name = f"{project_name}.Tests"

    # Find the test project in the list
    test_project = None
    for p in projects:
        if p['name'] == test_project_name and p['isTestProject']:
            test_project = p
            break

    if not test_project:
        # No test project found, skip this project
        continue

    # Run coverage for the test project
    try:
        result = subprocess.run(
            [f"{script_dir}/run-project-coverage.sh", test_project['path'], "1000"],
            capture_output=True,
            text=True,
            check=True
        )

        files = json.loads(result.stdout)

        # Filter files based on criteria
        for file in files:
            not_covered = file['lines']['notCovered']
            covered = file['lines']['covered']
            path = file['path']

            # Skip if notCovered is 0
            if not_covered == 0:
                continue

            # Skip if path contains /obj/
            if '/obj/' in path:
                continue

            # Calculate coverage ratio
            total_lines = covered + not_covered
            if total_lines > 0:
                coverage_ratio = covered / total_lines

                # Skip if coverage is >= 80%
                if coverage_ratio >= 0.8:
                    continue

            # Add to results
            all_files.append(file)

            # Check if we've reached the limit
            if len(all_files) >= limit:
                break

        # Break out of project loop if we've reached the limit
        if len(all_files) >= limit:
            break

    except subprocess.CalledProcessError:
        # If coverage fails for this project, continue to next
        continue

# Sort all files by notCovered (descending)
all_files.sort(key=lambda x: (-x['lines']['notCovered'], x['name']))

# Truncate to limit
all_files = all_files[:limit]

# Output JSON
print(json.dumps(all_files, indent=2))

EOF

# Run the Python script
echo "$PROJECTS_JSON" | python3 "$TEMP_SCRIPT" "$LIMIT" "$SCRIPT_DIR"

# Clean up
rm -f "$TEMP_SCRIPT"