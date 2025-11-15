#!/bin/zsh

# Run code coverage for a single project and return JSON array of file coverage
# Usage: ./run-project-coverage.sh <project-path> [limit]
# Output: JSON array with format [{ name: "ClassName", path: "relative/path", lines: {covered: 123, notCovered: 123} }]
# Ordered by notCovered (descending)

set -e

# Get the script directory and repository root
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Parse arguments
PROJECT_PATH="$1"
LIMIT="${2:-5}"  # Default limit is 5

if [ -z "$PROJECT_PATH" ]; then
    echo "Error: Project path is required" >&2
    echo "Usage: $0 <project-path> [limit]" >&2
    exit 1
fi

# Convert to absolute path if relative
if [[ "$PROJECT_PATH" != /* ]]; then
    PROJECT_PATH="$REPO_ROOT/$PROJECT_PATH"
fi

# Check if project file exists
if [ ! -f "$PROJECT_PATH" ]; then
    echo "Error: Project file not found: $PROJECT_PATH" >&2
    exit 1
fi

# Get project name
PROJECT_NAME=$(basename "$PROJECT_PATH" .csproj)

# Create temporary coverage directory
COVERAGE_DIR="$REPO_ROOT/coverage/$PROJECT_NAME"
rm -rf "$COVERAGE_DIR"
mkdir -p "$COVERAGE_DIR"

# Run tests with code coverage silently
cd "$REPO_ROOT/src"
dotnet test "$PROJECT_PATH" \
    --configuration Release \
    --collect:"XPlat Code Coverage" \
    --results-directory:"$COVERAGE_DIR" \
    --verbosity quiet \
    --nologo \
    > /dev/null 2>&1

# Find the coverage.cobertura.xml file
COVERAGE_FILE=$(find "$COVERAGE_DIR" -name "coverage.cobertura.xml" | head -1)

if [ -z "$COVERAGE_FILE" ]; then
    echo "Error: No coverage file found" >&2
    exit 1
fi

# Parse the coverage XML and generate JSON using Python
python3 - "$COVERAGE_FILE" "$LIMIT" "$REPO_ROOT" <<'PYTHON_SCRIPT'
import sys
import xml.etree.ElementTree as ET
import json
import os

coverage_file = sys.argv[1]
limit = int(sys.argv[2])
repo_root = sys.argv[3]

# Parse the coverage XML
tree = ET.parse(coverage_file)
root = tree.getroot()

# Get source directories from the coverage file
sources = []
for source in root.findall('.//source'):
    sources.append(source.text)

# Extract coverage data for each class
results = []

for package in root.findall('.//package'):
    for cls in package.findall('.//class'):
        class_name = cls.get('name', '')
        filename = cls.get('filename', '')

        # Skip if no filename
        if not filename:
            continue

        # Count covered and not covered lines
        covered = 0
        not_covered = 0

        for line in cls.findall('.//line'):
            hits = int(line.get('hits', '0'))
            if hits > 0:
                covered += 1
            else:
                not_covered += 1

        # Construct full path from source directories
        full_path = None
        for source in sources:
            candidate = os.path.join(source, filename)
            if os.path.exists(candidate):
                full_path = candidate
                break

        if not full_path:
            # If not found in sources, try as-is
            full_path = filename

        # Make path relative to repo root
        if full_path.startswith(repo_root + '/'):
            relative_path = full_path[len(repo_root) + 1:]
        else:
            relative_path = filename

        results.append({
            'name': class_name,
            'path': relative_path,
            'lines': {
                'covered': covered,
                'notCovered': not_covered
            }
        })

# Sort by notCovered (descending)
results.sort(key=lambda x: x['lines']['notCovered'], reverse=True)

# Apply limit
results = results[:limit]

# Output JSON
print(json.dumps(results, indent=2))
PYTHON_SCRIPT