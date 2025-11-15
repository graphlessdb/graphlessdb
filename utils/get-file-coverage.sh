#!/bin/bash

# Get coverage for a specific file
# Usage: ./get-file-coverage.sh <file-path>
# Output: JSON object with target file, test file, and coverage information

set -e

# Get the script directory and repository root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Validate arguments
if [ $# -lt 1 ]; then
    echo "Error: File path is required" >&2
    echo "Usage: $0 <file-path>" >&2
    exit 1
fi

TARGET_FILE="$1"

# Convert to absolute path if relative
if [[ "$TARGET_FILE" != /* ]]; then
    TARGET_FILE="$REPO_ROOT/$TARGET_FILE"
fi

# Validate target file exists
if [ ! -f "$TARGET_FILE" ]; then
    echo "Error: Target file not found: $TARGET_FILE" >&2
    exit 1
fi

# Get the target file name and path relative to repo root
TARGET_NAME=$(basename "$TARGET_FILE" .cs)
TARGET_REL_PATH=$(realpath --relative-to="$REPO_ROOT" "$TARGET_FILE" 2>/dev/null || python3 -c "import os, sys; print(os.path.relpath(sys.argv[1], sys.argv[2]))" "$TARGET_FILE" "$REPO_ROOT")

# Determine the project name from the target file path
# Pattern: src/ProjectName/...
PROJECT_NAME=""
if [[ "$TARGET_REL_PATH" =~ ^src/([^/]+)/ ]]; then
    PROJECT_NAME="${BASH_REMATCH[1]}"
fi

if [ -z "$PROJECT_NAME" ]; then
    echo "Error: Could not determine project name from path: $TARGET_REL_PATH" >&2
    exit 1
fi

# Find the corresponding test project
TEST_PROJECT_NAME="${PROJECT_NAME}.Tests"
TEST_PROJECT_PATH="$REPO_ROOT/src/$TEST_PROJECT_NAME/${TEST_PROJECT_NAME}.csproj"

if [ ! -f "$TEST_PROJECT_PATH" ]; then
    # No test project found - return result with no test file and zero coverage
    echo "{\"target\":{\"name\":\"$TARGET_NAME\",\"path\":\"$TARGET_REL_PATH\"},\"test\":{\"name\":\"\",\"path\":\"\"},\"lines\":{\"covered\":0,\"notCovered\":0}}"
    exit 0
fi

# Look for the test file
# Common patterns: <ClassName>Tests.cs, <ClassName>Test.cs
TEST_FILE=""
TEST_FILE_REL_PATH=""

# Search for test files matching the pattern
for pattern in "${TARGET_NAME}Tests.cs" "${TARGET_NAME}Test.cs"; do
    FOUND_FILE=$(find "$REPO_ROOT/src/$TEST_PROJECT_NAME" -name "$pattern" -type f 2>/dev/null | head -1)
    if [ -n "$FOUND_FILE" ]; then
        TEST_FILE="$FOUND_FILE"
        TEST_FILE_REL_PATH=$(realpath --relative-to="$REPO_ROOT" "$TEST_FILE" 2>/dev/null || python3 -c "import os, sys; print(os.path.relpath(sys.argv[1], sys.argv[2]))" "$TEST_FILE" "$REPO_ROOT")
        break
    fi
done

# If no test file found, return result with empty test info
if [ -z "$TEST_FILE" ]; then
    echo "{\"target\":{\"name\":\"$TARGET_NAME\",\"path\":\"$TARGET_REL_PATH\"},\"test\":{\"name\":\"\",\"path\":\"\"},\"lines\":{\"covered\":0,\"notCovered\":0}}"
    exit 0
fi

# Create unique coverage directory
TIMESTAMP=$(date +%s)
UNIQUE_ID="${TEST_PROJECT_NAME}_${TARGET_NAME}_${TIMESTAMP}_$$"
COVERAGE_DIR="$REPO_ROOT/.coverage/$UNIQUE_ID"
mkdir -p "$COVERAGE_DIR"

# Get test file name without extension
TEST_NAME=$(basename "$TEST_FILE" .cs)

# Run tests with code coverage, filtering to just the specific test class
cd "$REPO_ROOT"
dotnet test "$TEST_PROJECT_PATH" \
    --filter "FullyQualifiedName~${TEST_NAME}" \
    --configuration Release \
    --collect:"XPlat Code Coverage" \
    --results-directory:"$COVERAGE_DIR" \
    --verbosity:quiet \
    --nologo \
    2>&1 | grep -v "^Test run for" | grep -v "^Microsoft" | grep -v "^VSTest" | grep -v "^Starting test" | grep -v "^A total of" | grep -v "^Passed!" | grep -v "^Attachments:" | grep -v "^  /" || true

# Find the coverage XML file
COVERAGE_FILE=$(find "$COVERAGE_DIR" -name "coverage.cobertura.xml" | head -1)

if [ ! -f "$COVERAGE_FILE" ]; then
    # No coverage file - return result with test file but zero coverage
    rm -rf "$COVERAGE_DIR"
    echo "{\"target\":{\"name\":\"$TARGET_NAME\",\"path\":\"$TARGET_REL_PATH\"},\"test\":{\"name\":\"$TEST_NAME\",\"path\":\"$TEST_FILE_REL_PATH\"},\"lines\":{\"covered\":0,\"notCovered\":0}}"
    exit 0
fi

# Parse the coverage XML file and extract coverage for the specific target file
RESULT=$(python3 - "$COVERAGE_FILE" "$REPO_ROOT" "$TARGET_REL_PATH" "$TARGET_NAME" "$TEST_NAME" "$TEST_FILE_REL_PATH" <<'PYTHON_SCRIPT'
import sys
import xml.etree.ElementTree as ET
import json
from pathlib import Path

coverage_file = sys.argv[1]
repo_root = sys.argv[2]
target_rel_path = sys.argv[3]
target_name = sys.argv[4]
test_name = sys.argv[5]
test_file_rel_path = sys.argv[6]

# Parse the XML file
tree = ET.parse(coverage_file)
root = tree.getroot()

# Extract source path prefix
sources = root.findall('.//source')
source_prefix = sources[0].text if sources else ""

# Find coverage data for the specific target file
covered = 0
not_covered = 0
found_class = False

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

    # Check if this is our target file (compare normalized paths)
    if Path(rel_path) == Path(target_rel_path):
        found_class = True
        # Count covered and not covered lines
        lines = cls.findall('.//line')

        for line in lines:
            hits = int(line.get('hits', 0))
            if hits > 0:
                covered += 1
            else:
                not_covered += 1

# Build result JSON
result = {
    'target': {
        'name': target_name,
        'path': target_rel_path
    },
    'test': {
        'name': test_name,
        'path': test_file_rel_path
    },
    'lines': {
        'covered': covered,
        'notCovered': not_covered
    }
}

# Output JSON
print(json.dumps(result))

PYTHON_SCRIPT
)

# Clean up coverage directory
rm -rf "$COVERAGE_DIR"

# Output the result
echo "$RESULT"
