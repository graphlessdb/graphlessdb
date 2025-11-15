#!/bin/zsh

# Get code coverage for a specific file
#
# This script takes a file path as input, finds its corresponding test file (if any),
# runs code coverage for the test project, and returns coverage information in JSON format.
#
# Usage: ./get-file-coverage.sh <file-path>
#
# Arguments:
#   file-path: Path to the target source file (can be relative or absolute)
#              Compatible with output from get-least-covered-file.sh
#
# Output: JSON with format:
#   {
#     "target": { "name": "ClassName", "path": "relative/path/to/file.cs" },
#     "test": { "name": "ClassNameTests", "path": "relative/path/to/test.cs" },
#     "lines": { "covered": 123, "notCovered": 456 }
#   }
#
# Notes:
#   - test field will be null if no corresponding test file is found
#   - lines field will be null if the file is not found in the coverage report
#   - Errors are returned as JSON with an "error" field

set -e

# Get the script directory and repository root
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Parse arguments
TARGET_FILE="$1"

if [ -z "$TARGET_FILE" ]; then
    echo '{"error":"Target file path is required"}' >&2
    exit 1
fi

# Convert to absolute path if relative
if [[ "$TARGET_FILE" != /* ]]; then
    TARGET_FILE="$REPO_ROOT/$TARGET_FILE"
fi

# Check if target file exists
if [ ! -f "$TARGET_FILE" ]; then
    echo "{\"error\":\"Target file not found: $TARGET_FILE\"}" >&2
    exit 1
fi

# Get the relative path from repo root
if [[ "$TARGET_FILE" == "$REPO_ROOT/"* ]]; then
    RELATIVE_PATH="${TARGET_FILE#$REPO_ROOT/}"
else
    RELATIVE_PATH="$TARGET_FILE"
fi

# Extract target file information
TARGET_NAME=$(basename "$TARGET_FILE" .cs)
TARGET_DIR=$(dirname "$TARGET_FILE")

# Determine the project the target file belongs to
# Navigate up from the file to find the .csproj file
CURRENT_DIR="$TARGET_DIR"
PROJECT_FILE=""
while [ "$CURRENT_DIR" != "$REPO_ROOT" ] && [ "$CURRENT_DIR" != "/" ]; do
    CSPROJ_FILES=("$CURRENT_DIR"/*.csproj(N))
    if [ ${#CSPROJ_FILES[@]} -gt 0 ]; then
        PROJECT_FILE="${CSPROJ_FILES[1]}"
        break
    fi
    CURRENT_DIR=$(dirname "$CURRENT_DIR")
done

if [ -z "$PROJECT_FILE" ]; then
    echo "{\"error\":\"Could not find project file for target\"}" >&2
    exit 1
fi

# Get project name
PROJECT_NAME=$(basename "$PROJECT_FILE" .csproj)

# Check if this is already a test project
if [[ "$PROJECT_NAME" == *".Tests" ]]; then
    echo "{\"error\":\"Target file is in a test project, not a source file\"}" >&2
    exit 1
fi

# Find the corresponding test project
TEST_PROJECT_FILE="$REPO_ROOT/src/${PROJECT_NAME}.Tests/${PROJECT_NAME}.Tests.csproj"

if [ ! -f "$TEST_PROJECT_FILE" ]; then
    echo "{\"error\":\"No test project found for $PROJECT_NAME\",\"target\":{\"name\":\"$TARGET_NAME\",\"path\":\"$RELATIVE_PATH\"},\"test\":null,\"lines\":null}" >&2
    exit 1
fi

# Determine the relative path of the target file within its project
PROJECT_DIR=$(dirname "$PROJECT_FILE")
if [[ "$TARGET_FILE" == "$PROJECT_DIR/"* ]]; then
    FILE_RELATIVE_TO_PROJECT="${TARGET_FILE#$PROJECT_DIR/}"
else
    FILE_RELATIVE_TO_PROJECT="$TARGET_NAME.cs"
fi

# Extract the directory path within the project (if any)
FILE_DIR_IN_PROJECT=$(dirname "$FILE_RELATIVE_TO_PROJECT")
if [ "$FILE_DIR_IN_PROJECT" = "." ]; then
    FILE_DIR_IN_PROJECT=""
fi

# Try to find corresponding test file
# Common patterns:
# 1. Same subdirectory structure with .Tests suffix: Graph.Services.Internal -> Graph.Services.Internal.Tests
# 2. In a Tests/ subdirectory
# 3. At the root of the test project
# 4. Anywhere in the test project with matching name
TEST_FILE_PATTERNS=()

# Pattern 1: If file is in a subdirectory, try subdirectory.Tests pattern
if [ -n "$FILE_DIR_IN_PROJECT" ]; then
    TEST_DIR="${FILE_DIR_IN_PROJECT}.Tests"
    TEST_FILE_PATTERNS+=("$REPO_ROOT/src/${PROJECT_NAME}.Tests/${TEST_DIR}/${TARGET_NAME}Tests.cs")
fi

# Pattern 2: Same subdirectory structure in Tests/ folder
if [ -n "$FILE_DIR_IN_PROJECT" ]; then
    TEST_FILE_PATTERNS+=("$REPO_ROOT/src/${PROJECT_NAME}.Tests/Tests/${FILE_DIR_IN_PROJECT}/${TARGET_NAME}Tests.cs")
fi

# Pattern 3: Direct mapping to same path
TEST_FILE_PATTERNS+=("$REPO_ROOT/src/${PROJECT_NAME}.Tests/${FILE_RELATIVE_TO_PROJECT%.cs}Tests.cs")

# Pattern 4: In Tests/ subdirectory
TEST_FILE_PATTERNS+=("$REPO_ROOT/src/${PROJECT_NAME}.Tests/Tests/${TARGET_NAME}Tests.cs")

# Pattern 5: At root of test project
TEST_FILE_PATTERNS+=("$REPO_ROOT/src/${PROJECT_NAME}.Tests/${TARGET_NAME}Tests.cs")

# Pattern 6: Search anywhere in test project (last resort, glob pattern)
TEST_FILE_PATTERNS+=("$REPO_ROOT/src/${PROJECT_NAME}.Tests/**/${TARGET_NAME}Tests.cs"(N))

TEST_FILE=""
TEST_FILE_RELATIVE=""
for pattern in "${TEST_FILE_PATTERNS[@]}"; do
    # Expand glob patterns
    matches=($~pattern)
    if [ ${#matches[@]} -gt 0 ] && [ -f "${matches[1]}" ]; then
        TEST_FILE="${matches[1]}"
        if [[ "$TEST_FILE" == "$REPO_ROOT/"* ]]; then
            TEST_FILE_RELATIVE="${TEST_FILE#$REPO_ROOT/}"
        else
            TEST_FILE_RELATIVE="$TEST_FILE"
        fi
        break
    fi
done

TEST_NAME=""
if [ -n "$TEST_FILE" ]; then
    TEST_NAME=$(basename "$TEST_FILE" .cs)
fi

# Create temporary coverage directory
COVERAGE_DIR="$REPO_ROOT/.coverage/${PROJECT_NAME}.Tests"
rm -rf "$COVERAGE_DIR"
mkdir -p "$COVERAGE_DIR"

# Run tests with code coverage silently
cd "$REPO_ROOT/src"
dotnet test "$TEST_PROJECT_FILE" \
    --configuration Release \
    --collect:"XPlat Code Coverage" \
    --results-directory:"$COVERAGE_DIR" \
    --verbosity quiet \
    --nologo \
    > /dev/null 2>&1

# Find the coverage.cobertura.xml file
COVERAGE_FILE=$(find "$COVERAGE_DIR" -name "coverage.cobertura.xml" | head -1)

if [ -z "$COVERAGE_FILE" ]; then
    echo "{\"error\":\"No coverage file generated\"}" >&2
    exit 1
fi

# Parse the coverage XML and extract data for the specific file using Python
python3 - "$COVERAGE_FILE" "$RELATIVE_PATH" "$TARGET_NAME" "$TEST_FILE_RELATIVE" "$TEST_NAME" "$REPO_ROOT" <<'PYTHON_SCRIPT'
import sys
import xml.etree.ElementTree as ET
import json
import os

coverage_file = sys.argv[1]
target_path = sys.argv[2]
target_name = sys.argv[3]
test_path = sys.argv[4] if sys.argv[4] else None
test_name = sys.argv[5] if sys.argv[5] else None
repo_root = sys.argv[6]

# Parse the coverage XML
tree = ET.parse(coverage_file)
root = tree.getroot()

# Get source directories from the coverage file
sources = []
for source in root.findall('.//source'):
    sources.append(source.text)

# Find the matching class in the coverage report
found = False
covered = 0
not_covered = 0

for package in root.findall('.//package'):
    for cls in package.findall('.//class'):
        class_name = cls.get('name', '')
        filename = cls.get('filename', '')

        # Skip if no filename
        if not filename:
            continue

        # Construct full path from source directories
        full_path = None
        for source in sources:
            candidate = os.path.join(source, filename)
            if os.path.exists(candidate):
                full_path = candidate
                break

        if not full_path:
            full_path = filename

        # Make path relative to repo root
        if full_path.startswith(repo_root + '/'):
            relative_path = full_path[len(repo_root) + 1:]
        else:
            relative_path = filename

        # Check if this matches our target file
        if relative_path == target_path or class_name == target_name:
            found = True

            # Count covered and not covered lines
            for line in cls.findall('.//line'):
                hits = int(line.get('hits', '0'))
                if hits > 0:
                    covered += 1
                else:
                    not_covered += 1

            break

    if found:
        break

# Build result
result = {
    'target': {
        'name': target_name,
        'path': target_path
    },
    'test': {
        'name': test_name,
        'path': test_path
    } if test_name and test_path else None,
    'lines': {
        'covered': covered,
        'notCovered': not_covered
    } if found else None
}

if not found:
    result['error'] = 'Target file not found in coverage report'

print(json.dumps(result, indent=2))
PYTHON_SCRIPT
