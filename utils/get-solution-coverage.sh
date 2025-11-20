#!/bin/bash

# Get code coverage for the entire solution
# Outputs JSON in format: { lineCoveragePercentage: <value>, branchCoveragePercentage: <value> }

# Disable MSBuild node reuse to prevent hanging processes
export MSBUILDDISABLENODEREUSE=1

# Navigate to project root
cd "$(dirname "$0")/.."

# Create unique coverage directory
COVERAGE_DIR=".coverage/solution-$(date +%s)-$$"
mkdir -p "$COVERAGE_DIR"

# Build the entire solution first
echo "Building solution..." >&2
dotnet build src/GraphlessDB.sln --nodereuse:false --configuration Debug --verbosity quiet
BUILD_EXIT=$?

if [ $BUILD_EXIT -ne 0 ]; then
  echo "Error: Build failed with exit code $BUILD_EXIT" >&2
  rm -rf "$COVERAGE_DIR"
  exit 1
fi

# Run all tests with coverage
# Note: dotnet test sometimes returns non-zero exit even when tests pass
echo "Running tests with code coverage..." >&2
dotnet test src/GraphlessDB.sln \
  --nodereuse:false \
  --collect:"XPlat Code Coverage" \
  --settings:"src/settings.runsettings" \
  --results-directory "$COVERAGE_DIR" \
  --verbosity quiet \
  --no-build \

# Find all coverage.cobertura.xml files
COVERAGE_FILES=$(find "$COVERAGE_DIR" -name "coverage.cobertura.xml" -type f)

if [ -z "$COVERAGE_FILES" ]; then
  echo "Error: No coverage files found" >&2
  rm -rf "$COVERAGE_DIR"
  exit 1
fi

# Parse all coverage XML files and aggregate the results
python3 - "$COVERAGE_FILES" <<'PYTHON_SCRIPT'
import sys
import xml.etree.ElementTree as ET
import json

# Get all coverage files from stdin (space-separated on single line)
coverage_files_str = sys.argv[1]
coverage_files = coverage_files_str.strip().split('\n')

# Aggregate totals across all coverage files
total_lines_covered = 0
total_lines_valid = 0
total_branches_covered = 0
total_branches_valid = 0

for coverage_file in coverage_files:
    coverage_file = coverage_file.strip()
    if not coverage_file:
        continue

    try:
        # Parse the XML
        tree = ET.parse(coverage_file)
        root = tree.getroot()

        # Get the aggregated attributes from the root element
        lines_covered = int(root.get('lines-covered', 0))
        lines_valid = int(root.get('lines-valid', 0))
        branches_covered = int(root.get('branches-covered', 0))
        branches_valid = int(root.get('branches-valid', 0))

        # Add to totals
        total_lines_covered += lines_covered
        total_lines_valid += lines_valid
        total_branches_covered += branches_covered
        total_branches_valid += branches_valid

    except Exception as e:
        print(f"Error parsing {coverage_file}: {e}", file=sys.stderr)
        continue

# Calculate percentages
if total_lines_valid > 0:
    line_coverage_pct = (total_lines_covered / total_lines_valid) * 100
else:
    line_coverage_pct = 0.0

if total_branches_valid > 0:
    branch_coverage_pct = (total_branches_covered / total_branches_valid) * 100
else:
    branch_coverage_pct = 0.0

# Output JSON format
result = {
    "lineCoveragePercentage": round(line_coverage_pct, 2),
    "branchCoveragePercentage": round(branch_coverage_pct, 2)
}
print(json.dumps(result))
PYTHON_SCRIPT

# Clean up coverage files
rm -rf "$COVERAGE_DIR"
