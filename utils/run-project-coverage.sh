#!/bin/zsh

# Run code coverage for a single project and return JSON-formatted results
# Usage: ./run-project-coverage.sh <project-path> [limit]
# Output: JSON array of files ordered by notCovered lines (descending)

set -e

# Get the script directory and repository root
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Validate arguments
if [ $# -lt 1 ]; then
    echo "Error: Project path is required" >&2
    echo "Usage: $0 <project-path> [limit]" >&2
    exit 1
fi

PROJECT_PATH="$1"
LIMIT="${2:-5}"

# Validate project path
if [ ! -f "$REPO_ROOT/$PROJECT_PATH" ]; then
    echo "Error: Project file not found: $PROJECT_PATH" >&2
    exit 1
fi

# Check if it's a test project
PROJECT_NAME=$(basename "$PROJECT_PATH" .csproj)
if [[ ! "$PROJECT_NAME" =~ \.Tests$ ]]; then
    echo "Error: Project is not a test project: $PROJECT_NAME" >&2
    exit 1
fi

# Create unique coverage directory
TIMESTAMP=$(date +%s)
UNIQUE_ID="${PROJECT_NAME}_${TIMESTAMP}_$$"
COVERAGE_DIR="$REPO_ROOT/.coverage/$UNIQUE_ID"
mkdir -p "$COVERAGE_DIR"

# Run tests with code coverage
cd "$REPO_ROOT"
dotnet test "$PROJECT_PATH" \
    --configuration Release \
    --collect:"XPlat Code Coverage" \
    --results-directory:"$COVERAGE_DIR" \
    --verbosity:quiet \
    --nologo \
    2>&1 | grep -v "^Test run for" | grep -v "^Microsoft" | grep -v "^VSTest" | grep -v "^Starting test" | grep -v "^A total of" | grep -v "^Passed!" | grep -v "^Attachments:" | grep -v "^  /" || true

# Find the coverage XML file
COVERAGE_FILE=$(find "$COVERAGE_DIR" -name "coverage.cobertura.xml" | head -1)

if [ ! -f "$COVERAGE_FILE" ]; then
    echo "Error: Coverage file not found" >&2
    rm -rf "$COVERAGE_DIR"
    exit 1
fi

# Parse the coverage XML file and generate JSON output using Python
RESULT=$(python3 - "$COVERAGE_FILE" "$REPO_ROOT" "$LIMIT" <<'PYTHON_SCRIPT'
import sys
import xml.etree.ElementTree as ET
import json
from pathlib import Path

coverage_file = sys.argv[1]
repo_root = sys.argv[2]
limit = int(sys.argv[3])

# Parse the XML file
tree = ET.parse(coverage_file)
root = tree.getroot()

# Extract source path prefix
sources = root.findall('.//source')
source_prefix = sources[0].text if sources else ""

# Collect coverage data for each class
results = []

for cls in root.findall('.//class'):
    class_name = cls.get('name')
    filename = cls.get('filename')

    if not filename:
        continue

    # Calculate full path relative to repo root
    if source_prefix and not filename.startswith('/'):
        full_path = str(Path(source_prefix) / filename)
    else:
        full_path = filename

    # Make path relative to repo root
    try:
        rel_path = str(Path(full_path).relative_to(repo_root))
    except ValueError:
        # If path is not relative to repo_root, use as-is
        rel_path = full_path

    # Count covered and not covered lines
    lines = cls.findall('.//line')
    covered = 0
    not_covered = 0

    for line in lines:
        hits = int(line.get('hits', 0))
        if hits > 0:
            covered += 1
        else:
            not_covered += 1

    results.append({
        'name': class_name,
        'path': rel_path,
        'lines': {
            'covered': covered,
            'notCovered': not_covered
        }
    })

# Sort by notCovered (descending), then by name for consistency
results.sort(key=lambda x: (-x['lines']['notCovered'], x['name']))

# Apply limit
results = results[:limit]

# Output JSON
print(json.dumps(results, indent=2))

PYTHON_SCRIPT
)

# Clean up coverage directory
rm -rf "$COVERAGE_DIR"

# Output the result
echo "$RESULT"