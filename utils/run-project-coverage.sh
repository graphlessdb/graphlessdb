#!/bin/bash

# Run code coverage for a specific project and return JSON with per-class coverage

set -e

# Default limit
LIMIT=5

# Check if project path is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <project-path> [limit]" >&2
    echo "Example: $0 GraphlessDB.Tests/GraphlessDB.Tests.csproj 10" >&2
    exit 1
fi

PROJECT_PATH="$1"

# Parse optional limit parameter
if [ $# -ge 2 ]; then
    LIMIT="$2"
fi

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Extract project name from path
PROJECT_NAME=$(basename "$PROJECT_PATH" .csproj)

# Change to the src directory
cd "$PROJECT_ROOT/src"

# Create coverage directory
COVERAGE_DIR="$PROJECT_ROOT/coverage/$PROJECT_NAME"
mkdir -p "$COVERAGE_DIR"

# Run tests with code coverage
dotnet test "$PROJECT_PATH" \
    --configuration Release \
    --collect:"XPlat Code Coverage" \
    --results-directory:"$COVERAGE_DIR" \
    --verbosity quiet > /dev/null 2>&1

# Find the coverage.cobertura.xml file
COVERAGE_FILE=$(find "$COVERAGE_DIR" -name "coverage.cobertura.xml" | head -1)

if [ -z "$COVERAGE_FILE" ]; then
    echo "[]"
    exit 0
fi

# Export the coverage file path and limit for the Python script
export COVERAGE_FILE
export LIMIT

# Parse the coverage XML and generate JSON output
python3 <<'EOF'
import xml.etree.ElementTree as ET
import json
import sys
import os

coverage_file = os.environ.get('COVERAGE_FILE')
limit = int(os.environ.get('LIMIT', '5'))

try:
    tree = ET.parse(coverage_file)
    root = tree.getroot()

    results = []

    # Iterate through all classes in the coverage report
    for package in root.findall('.//package'):
        for class_elem in package.findall('.//class'):
            class_name = class_elem.get('name', '')

            # Count covered and not covered lines
            covered = 0
            not_covered = 0

            for line in class_elem.findall('.//lines/line'):
                hits = int(line.get('hits', '0'))
                if hits > 0:
                    covered += 1
                else:
                    not_covered += 1

            # Only include classes that have lines
            if covered > 0 or not_covered > 0:
                results.append({
                    'name': class_name,
                    'lines': {
                        'covered': covered,
                        'notCovered': not_covered
                    }
                })

    # Sort by notCovered descending (highest first)
    results.sort(key=lambda x: x['lines']['notCovered'], reverse=True)

    # Apply limit
    results = results[:limit]

    # Output JSON
    print(json.dumps(results, indent=2))

except Exception as e:
    # In case of error, output empty array
    print('[]')
    sys.exit(0)
EOF
