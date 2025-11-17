#!/bin/bash

# Create GitHub issues for least covered files
# Usage: ./create-unit-test-issues.sh [limit]

set -e

LIMIT=${1:-10}

# Navigate to project root
cd "$(dirname "$0")/.."

# Get least covered files
FILES_JSON=$("$(dirname "$0")/get-least-covered-files.sh" "$LIMIT")

# Create issues using gh CLI
echo "$FILES_JSON" | python3 - <<'PYTHON_SCRIPT'
import sys
import json
import subprocess

files_json = sys.stdin.read()
files = json.loads(files_json)

for file_info in files:
    name = file_info['name']
    title = f"Create unit tests for {name}"
    body = json.dumps(file_info, indent=2)

    # Create GitHub issue
    try:
        result = subprocess.run(
            ['gh', 'issue', 'create', '--title', title, '--body', body],
            capture_output=True,
            text=True,
            check=True
        )
        print(f"Created issue: {title}")
        print(f"  URL: {result.stdout.strip()}")
    except subprocess.CalledProcessError as e:
        print(f"Error creating issue for {name}: {e.stderr}")
PYTHON_SCRIPT
