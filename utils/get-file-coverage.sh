#!/bin/zsh

# Get code coverage for a specific file
# Usage: ./get-file-coverage.sh <file-path>
# Output: JSON object with coverage information

set -e

# Get the script directory and repository root
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Validate arguments
if [ $# -lt 1 ]; then
    echo "Error: File path is required" >&2
    echo "Usage: $0 <file-path>" >&2
    exit 1
fi

TARGET_FILE="$1"

# Remove leading ./ if present
TARGET_FILE="${TARGET_FILE#./}"

# Make sure path is absolute
if [[ ! "$TARGET_FILE" = /* ]]; then
    TARGET_FILE="$REPO_ROOT/$TARGET_FILE"
fi

# Validate target file exists
if [ ! -f "$TARGET_FILE" ]; then
    echo "Error: Target file not found: $TARGET_FILE" >&2
    exit 1
fi

# Extract file name and relative path
TARGET_FILENAME=$(basename "$TARGET_FILE")
TARGET_NAME="${TARGET_FILENAME%.cs}"
TARGET_REL_PATH=$(realpath --relative-to="$REPO_ROOT" "$TARGET_FILE" 2>/dev/null || python3 -c "import os.path; print(os.path.relpath('$TARGET_FILE', '$REPO_ROOT'))")

# Count total lines in target file (excluding empty lines for better accuracy)
TOTAL_LINES=$(grep -c '.' "$TARGET_FILE" || echo "0")

# Determine the test file name
TEST_FILENAME="${TARGET_NAME}Tests.cs"

# Search for the test file in test projects
TEST_FILE=""
for TEST_DIR in "$REPO_ROOT/src/GraphlessDB.Tests" "$REPO_ROOT/src/GraphlessDB.DynamoDB.Tests"; do
    if [ -d "$TEST_DIR" ]; then
        FOUND=$(find "$TEST_DIR" -name "$TEST_FILENAME" -type f | head -1)
        if [ -n "$FOUND" ]; then
            TEST_FILE="$FOUND"
            break
        fi
    fi
done

# If no test file found, return JSON with no test and zero coverage
if [ -z "$TEST_FILE" ]; then
    cat <<EOF
{"target":{"name":"$TARGET_NAME","path":"$TARGET_REL_PATH"},"test":null,"lines":{"covered":0,"notCovered":$TOTAL_LINES}}
EOF
    exit 0
fi

# Extract test file details
TEST_REL_PATH=$(realpath --relative-to="$REPO_ROOT" "$TEST_FILE" 2>/dev/null || python3 -c "import os.path; print(os.path.relpath('$TEST_FILE', '$REPO_ROOT'))")
TEST_NAME=$(basename "$TEST_FILE" .cs)

# Find which test project contains this test file
TEST_PROJECT=""
if [[ "$TEST_FILE" == *"/GraphlessDB.Tests/"* ]]; then
    TEST_PROJECT="$REPO_ROOT/src/GraphlessDB.Tests/GraphlessDB.Tests.csproj"
elif [[ "$TEST_FILE" == *"/GraphlessDB.DynamoDB.Tests/"* ]]; then
    TEST_PROJECT="$REPO_ROOT/src/GraphlessDB.DynamoDB.Tests/GraphlessDB.DynamoDB.Tests.csproj"
fi

if [ -z "$TEST_PROJECT" ] || [ ! -f "$TEST_PROJECT" ]; then
    echo "Error: Could not determine test project for test file: $TEST_FILE" >&2
    exit 1
fi

# Create unique coverage directory
TIMESTAMP=$(date +%s)
UNIQUE_ID="file_coverage_${TIMESTAMP}_$$"
COVERAGE_DIR="$REPO_ROOT/.coverage/$UNIQUE_ID"
mkdir -p "$COVERAGE_DIR"

# Run tests with code coverage, filtering to only the specific test class
cd "$REPO_ROOT"

# Extract the namespace and class from the test file to create a filter
TEST_NAMESPACE=$(grep -E "^namespace " "$TEST_FILE" | head -1 | sed 's/namespace //' | sed 's/[[:space:]]*$//' | tr -d '\r')
FULL_TEST_NAME="${TEST_NAMESPACE}.${TEST_NAME}"

dotnet test "$TEST_PROJECT" \
    --filter "FullyQualifiedName~${FULL_TEST_NAME}" \
    --configuration Release \
    --collect:"XPlat Code Coverage" \
    --results-directory:"$COVERAGE_DIR" \
    --verbosity:quiet \
    --nologo \
    > /dev/null 2>&1 || true

# Find the coverage XML file
COVERAGE_FILE=$(find "$COVERAGE_DIR" -name "coverage.cobertura.xml" | head -1)

if [ ! -f "$COVERAGE_FILE" ]; then
    # No coverage file means tests didn't run or no coverage collected
    # Return zero coverage
    rm -rf "$COVERAGE_DIR"
    cat <<EOF
{"target":{"name":"$TARGET_NAME","path":"$TARGET_REL_PATH"},"test":{"name":"$TEST_NAME","path":"$TEST_REL_PATH"},"lines":{"covered":0,"notCovered":$TOTAL_LINES}}
EOF
    exit 0
fi

# Parse the coverage XML file for the specific target file
RESULT=$(python3 - "$COVERAGE_FILE" "$TARGET_FILE" "$TARGET_NAME" "$TARGET_REL_PATH" "$TEST_NAME" "$TEST_REL_PATH" "$TOTAL_LINES" <<'PYTHON_SCRIPT'
import sys
import xml.etree.ElementTree as ET
import json
import os

coverage_file = sys.argv[1]
target_file = sys.argv[2]
target_name = sys.argv[3]
target_rel_path = sys.argv[4]
test_name = sys.argv[5]
test_rel_path = sys.argv[6]
total_lines = int(sys.argv[7])

# Parse the XML file
tree = ET.parse(coverage_file)
root = tree.getroot()

# Find the class matching our target file
covered = 0
not_covered = 0
found = False

for cls in root.findall('.//class'):
    filename = cls.get('filename')

    if not filename:
        continue

    # Normalize paths for comparison
    normalized_filename = os.path.normpath(filename)
    normalized_target = os.path.normpath(target_file)

    # Check if this is our target file
    if normalized_filename == normalized_target or filename.endswith('/' + os.path.basename(target_file)):
        found = True

        # Count covered and not covered lines
        lines = cls.findall('.//line')
        for line in lines:
            hits = int(line.get('hits', 0))
            if hits > 0:
                covered += 1
            else:
                not_covered += 1
        break

# If target file not found in coverage report, use total lines as not covered
if not found:
    not_covered = total_lines

# Build the result JSON
result = {
    'target': {
        'name': target_name,
        'path': target_rel_path
    },
    'test': {
        'name': test_name,
        'path': test_rel_path
    },
    'lines': {
        'covered': covered,
        'notCovered': not_covered
    }
}

print(json.dumps(result))

PYTHON_SCRIPT
)

# Clean up coverage directory
rm -rf "$COVERAGE_DIR"

# Output the result
echo "$RESULT"
