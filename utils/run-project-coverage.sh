#!/bin/bash

# Run code coverage for a single project
# Usage: ./run-project-coverage.sh <project-path> [limit]
# Output: JSON array of files with coverage info

set -e

if [ $# -eq 0 ]; then
  echo "Usage: $0 <project-path> [limit]"
  exit 1
fi

PROJECT_PATH=$1
LIMIT=${2:-5}

# Navigate to project root
cd "$(dirname "$0")/.."

# Ensure we're on main branch with latest code
git checkout main > /dev/null 2>&1 || true
git pull > /dev/null 2>&1 || true

# Build the entire solution first
dotnet build src/GraphlessDB.sln --verbosity quiet > /dev/null 2>&1

# Create unique coverage directory
COVERAGE_DIR=".coverage/project-$(date +%s)"
mkdir -p "$COVERAGE_DIR"

# Run tests with coverage for the specific project
dotnet test "$PROJECT_PATH" \
  --collect:"XPlat Code Coverage" \
  --results-directory "$COVERAGE_DIR" \
  --no-build \
  --verbosity quiet \
  > /dev/null 2>&1

# Find the coverage.cobertura.xml file
COVERAGE_FILE=$(find "$COVERAGE_DIR" -name "coverage.cobertura.xml" | head -n 1)

if [ -z "$COVERAGE_FILE" ]; then
  echo "[]"
  rm -rf "$COVERAGE_DIR"
  exit 0
fi

# Parse coverage XML and extract file-level coverage
# Using Python for more reliable XML parsing
python3 - "$COVERAGE_FILE" "$LIMIT" <<'PYTHON_SCRIPT'
import sys
import xml.etree.ElementTree as ET
import json
import os

coverage_file = sys.argv[1]
limit = int(sys.argv[2])

tree = ET.parse(coverage_file)
root = tree.getroot()

results = []

# Iterate through all classes in the coverage report
for package in root.findall('.//package'):
    for cls in package.findall('.//class'):
        filename = cls.get('filename', '')
        class_name = cls.get('name', '')

        # Normalize path to be relative from project root
        # Handle cases where it might be absolute or partially qualified
        if 'graphlessdb/src/' in filename:
            # Extract from src/ onwards
            filename = 'src/' + filename.split('graphlessdb/src/', 1)[1]
        elif filename.startswith('/'):
            # If absolute path, try to make it relative
            filename = os.path.relpath(filename, os.getcwd())

        # Skip compiler-generated classes and files
        if '<' in class_name or '>' in class_name or class_name.startswith('__'):
            continue
        if '/obj/' in filename or '\\obj\\' in filename:
            continue
        if '.g.cs' in filename or 'Generated' in filename:
            continue

        # Calculate line coverage
        lines = cls.findall('.//line')
        covered = sum(1 for line in lines if int(line.get('hits', 0)) > 0)
        not_covered = sum(1 for line in lines if int(line.get('hits', 0)) == 0)

        if covered + not_covered == 0:
            continue

        results.append({
            'name': class_name,
            'path': filename,
            'lines': {
                'covered': covered,
                'notCovered': not_covered
            }
        })

# Sort by notCovered descending
results.sort(key=lambda x: x['lines']['notCovered'], reverse=True)

# Limit results
results = results[:limit]

print(json.dumps(results, indent=2))
PYTHON_SCRIPT

# Clean up coverage files
rm -rf "$COVERAGE_DIR"
