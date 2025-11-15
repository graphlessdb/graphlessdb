#!/bin/bash

# Get coverage for a specific file
# Usage: ./get-file-coverage.sh <file-path>
# Output: Coverage report for the file or a message if no test coverage exists

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

INPUT_FILE="$1"

# Normalize the input file path to be relative to repo root
if [[ "$INPUT_FILE" = /* ]]; then
    # Absolute path - make it relative to repo root
    TARGET_FILE="${INPUT_FILE#$REPO_ROOT/}"
else
    # Already relative - remove leading ./ if present
    TARGET_FILE="${INPUT_FILE#./}"
fi

# Extract the file name without extension and directory
FILE_NAME=$(basename "$TARGET_FILE")
FILE_NAME_NO_EXT="${FILE_NAME%.*}"
FILE_DIR=$(dirname "$TARGET_FILE")

# Determine the project name from the file path
# e.g., src/GraphlessDB/Connection.cs -> GraphlessDB
if [[ "$TARGET_FILE" =~ ^src/([^/]+)/ ]]; then
    PROJECT_NAME="${BASH_REMATCH[1]}"
else
    echo "Error: Unable to determine project name from file path: $TARGET_FILE" >&2
    exit 1
fi

# Determine the test project name
TEST_PROJECT_NAME="${PROJECT_NAME}.Tests"
TEST_PROJECT_PATH="src/${TEST_PROJECT_NAME}/${TEST_PROJECT_NAME}.csproj"

# Check if test project exists
if [ ! -f "$REPO_ROOT/$TEST_PROJECT_PATH" ]; then
    echo "No test coverage available for $TARGET_FILE"
    exit 0
fi

# Look for the corresponding test file
# Common patterns: <FileName>Tests.cs or <FileName>Test.cs
TEST_FILE_PATTERN="${FILE_NAME_NO_EXT}Test"
TEST_FILES=$(find "$REPO_ROOT/src/$TEST_PROJECT_NAME" -type f -name "${TEST_FILE_PATTERN}s.cs" -o -name "${TEST_FILE_PATTERN}.cs" 2>/dev/null || true)

if [ -z "$TEST_FILES" ]; then
    echo "No test coverage available for $TARGET_FILE"
    exit 0
fi

# Create unique coverage directory
TIMESTAMP=$(date +%s)
UNIQUE_ID="${TEST_PROJECT_NAME}_${TIMESTAMP}_$$"
COVERAGE_DIR="$REPO_ROOT/.coverage/$UNIQUE_ID"
mkdir -p "$COVERAGE_DIR"

# Function to clean up on exit
cleanup() {
    if [ -d "$COVERAGE_DIR" ]; then
        rm -rf "$COVERAGE_DIR"
    fi
}

# Set trap to cleanup on exit or error
trap cleanup EXIT INT TERM

# Run tests with code coverage for the test project
cd "$REPO_ROOT"
dotnet test "$TEST_PROJECT_PATH" \
    --configuration Release \
    --collect:"XPlat Code Coverage" \
    --results-directory:"$COVERAGE_DIR" \
    --verbosity:quiet \
    --nologo \
    > /dev/null 2>&1

# Find the coverage XML file
COVERAGE_FILE=$(find "$COVERAGE_DIR" -name "coverage.cobertura.xml" | head -1)

if [ ! -f "$COVERAGE_FILE" ]; then
    echo "Error: Coverage file not found" >&2
    exit 1
fi

# Parse the coverage XML file and extract coverage for the target file using Python
python3 - "$COVERAGE_FILE" "$TARGET_FILE" "$REPO_ROOT" <<'PYTHON_SCRIPT'
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
import os

coverage_file = sys.argv[1]
target_file = sys.argv[2]
repo_root = sys.argv[3]

# Parse the XML file
tree = ET.parse(coverage_file)
root = tree.getroot()

# Extract source path prefix
sources = root.findall('.//source')
source_prefix = sources[0].text if sources else ""

# Normalize target file for matching
target_file_normalized = target_file.strip().replace('\\', '/')
target_file_name = os.path.basename(target_file_normalized)

# Find the class(es) that match the target file
matched_classes = []

for cls in root.findall('.//class'):
    filename = cls.get('filename')

    if not filename:
        continue

    # Normalize the filename from coverage
    filename_normalized = filename.replace('\\', '/')

    # Try multiple matching strategies:
    # 1. Exact match on full path
    # 2. Match on relative path
    # 3. Match on filename only

    matches = False

    # Strategy 1: Exact match
    if filename_normalized == target_file_normalized:
        matches = True

    # Strategy 2: Relative path match
    if not matches and source_prefix:
        full_path = os.path.join(source_prefix, filename_normalized).replace('\\', '/')
        try:
            rel_path = str(Path(full_path).relative_to(repo_root))
            if rel_path == target_file_normalized:
                matches = True
        except ValueError:
            pass

    # Strategy 3: Filename match
    if not matches:
        coverage_file_name = os.path.basename(filename_normalized)
        if coverage_file_name == target_file_name:
            matches = True

    # Strategy 4: Check if target file ends with the coverage filename path
    if not matches:
        if target_file_normalized.endswith(filename_normalized):
            matches = True

    # Strategy 5: Check if coverage filename ends with the target file path
    if not matches:
        if filename_normalized.endswith(target_file_normalized):
            matches = True

    if matches:
        matched_classes.append(cls)

if not matched_classes:
    print(f"No coverage data found for {target_file}")
    sys.exit(0)

# Calculate and display coverage for matched classes
total_lines = 0
covered_lines = 0
not_covered_lines = 0

for cls in matched_classes:
    class_name = cls.get('name')
    line_rate = float(cls.get('line-rate', 0))
    branch_rate = float(cls.get('branch-rate', 0))

    # Count lines
    lines = cls.findall('.//line')
    class_total = len(lines)
    class_covered = sum(1 for line in lines if int(line.get('hits', 0)) > 0)
    class_not_covered = class_total - class_covered

    total_lines += class_total
    covered_lines += class_covered
    not_covered_lines += class_not_covered

# Calculate percentages
line_coverage_pct = 0
if total_lines > 0:
    line_coverage_pct = round((covered_lines / total_lines) * 100, 2)

# Display coverage summary
print(f"Coverage report for: {target_file}")
print(f"{'='*60}")
print(f"Lines:        {covered_lines}/{total_lines} covered ({line_coverage_pct}%)")
print(f"Not covered:  {not_covered_lines}")
print()

# Display detailed line-by-line coverage
for cls in matched_classes:
    class_name = cls.get('name')
    print(f"Class: {class_name}")
    print(f"{'-'*60}")

    lines = cls.findall('.//line')
    # Sort by line number
    lines_sorted = sorted(lines, key=lambda x: int(x.get('number', 0)))

    for line in lines_sorted:
        line_num = line.get('number')
        hits = int(line.get('hits', 0))
        status = "✓ COVERED" if hits > 0 else "✗ NOT COVERED"
        hit_count = f"(hits: {hits})" if hits > 0 else ""

        print(f"  Line {line_num:>5}: {status} {hit_count}")

    print()

PYTHON_SCRIPT
