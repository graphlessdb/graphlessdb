#!/bin/bash

# Get coverage for a specific file
# Usage: ./get-file-coverage.sh <file-path>

set -e

# Disable MSBuild node reuse to prevent hanging processes
export MSBUILDDISABLENODEREUSE=1

if [ $# -eq 0 ]; then
  echo "Usage: $0 <file-path>"
  exit 1
fi

TARGET_FILE=$1

# Navigate to project root
cd "$(dirname "$0")/.."

# Normalize the target file path (remove leading ./ and get absolute path if needed)
if [[ "$TARGET_FILE" == /* ]]; then
  # Already absolute
  TARGET_FILE_ABS="$TARGET_FILE"
else
  # Make it absolute
  TARGET_FILE_ABS="$(pwd)/$TARGET_FILE"
fi

# Extract the base filename for matching
TARGET_BASENAME=$(basename "$TARGET_FILE")
TARGET_NAME="${TARGET_BASENAME%.cs}"

# Try to find a corresponding test file
# Common patterns: Foo.cs -> FooTests.cs, Foo.cs -> Foo.Tests.cs
TEST_FILE=""

# Search for test files
for pattern in "${TARGET_NAME}Tests.cs" "${TARGET_NAME}.Tests.cs" "${TARGET_NAME}Test.cs"; do
  found=$(find src -name "$pattern" -path "*.Tests/*" -type f | head -n 1)
  if [ ! -z "$found" ]; then
    TEST_FILE="$found"
    break
  fi
done

if [ -z "$TEST_FILE" ]; then
  echo "Target file has no test coverage."
  exit 0
fi

# Find the test project containing this test file
TEST_PROJECT=$(find src -name "*.Tests.csproj" -type f | while read proj; do
  proj_dir=$(dirname "$proj")
  if [[ "$TEST_FILE" == "$proj_dir"* ]]; then
    echo "$proj"
    break
  fi
done | head -n 1)

if [ -z "$TEST_PROJECT" ]; then
  echo "Target file has no test coverage."
  exit 0
fi

# Build the solution first
dotnet build src/GraphlessDB.sln --nodereuse:false --verbosity quiet > /dev/null 2>&1

# Create unique coverage directory
COVERAGE_DIR=".coverage/file-$(date +%s)"
mkdir -p "$COVERAGE_DIR"

# Run tests with coverage for the test project
dotnet test "$TEST_PROJECT" \
  --nodereuse:false \
  --collect:"XPlat Code Coverage" \
  --settings:"src/settings.runsettings" \
  --results-directory "$COVERAGE_DIR" \
  --no-build \
  --verbosity quiet \
  > /dev/null 2>&1

# Find the coverage.cobertura.xml file
COVERAGE_FILE=$(find "$COVERAGE_DIR" -name "coverage.cobertura.xml" | head -n 1)

if [ -z "$COVERAGE_FILE" ]; then
  echo "No coverage data generated."
  rm -rf "$COVERAGE_DIR"
  exit 0
fi

# Extract coverage for the specific file
python3 - "$COVERAGE_FILE" "$TARGET_FILE" "$TARGET_BASENAME" <<'PYTHON_SCRIPT'
import sys
import xml.etree.ElementTree as ET
import os

coverage_file = sys.argv[1]
target_file = sys.argv[2]
target_basename = sys.argv[3]

tree = ET.parse(coverage_file)
root = tree.getroot()

found = False

# Look for the target file in the coverage report
for cls in root.findall('.//class'):
    filename = cls.get('filename', '')

    # Match by basename or full path
    if os.path.basename(filename) == target_basename or filename == target_file or filename.endswith(target_file):
        found = True
        class_name = cls.get('name', '')

        print(f"Coverage for {target_basename}:")
        print(f"Class: {class_name}")
        print(f"File: {filename}")
        print()

        # Get line-by-line coverage
        lines = cls.findall('.//line')
        covered_lines = []
        uncovered_lines = []

        for line in lines:
            line_num = line.get('number')
            hits = int(line.get('hits', 0))

            if hits > 0:
                covered_lines.append(line_num)
            else:
                uncovered_lines.append(line_num)

        print(f"Lines covered: {len(covered_lines)}")
        print(f"Lines not covered: {len(uncovered_lines)}")

        if len(covered_lines) + len(uncovered_lines) > 0:
            coverage_pct = (len(covered_lines) / (len(covered_lines) + len(uncovered_lines))) * 100
            print(f"Coverage percentage: {coverage_pct:.2f}%")

        if uncovered_lines:
            print()
            print("Uncovered lines:")
            # Show first 20 uncovered lines
            for line_num in uncovered_lines[:20]:
                print(f"  Line {line_num}")
            if len(uncovered_lines) > 20:
                print(f"  ... and {len(uncovered_lines) - 20} more")

        print()
        break

if not found:
    print(f"Target file {target_basename} not found in coverage report.")
PYTHON_SCRIPT

# Clean up coverage files
rm -rf "$COVERAGE_DIR"
