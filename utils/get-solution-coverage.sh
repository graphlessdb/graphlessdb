#!/bin/bash

# Run code coverage for all unit tests in the solution
# Outputs JSON in the format: { lineCoveragePercentage: 0, branchCoveragePercentage: 0 }

set -e

# Disable MSBuild node reuse to prevent hanging processes
export MSBUILDDISABLENODEREUSE=1

# Navigate to project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT"

# Create unique coverage directory
COVERAGE_DIR=".coverage/solution-$(date +%s)-$$"
mkdir -p "$COVERAGE_DIR"

# Cleanup function
cleanup() {
  rm -rf "$COVERAGE_DIR"
}

# Trap errors and cleanup
trap cleanup EXIT

# Build the solution first (suppress output unless there's an error)
BUILD_OUTPUT=$(dotnet build src/GraphlessDB.sln \
  --no-incremental \
  -p:UseSharedCompilation=false \
  -p:UseRazorBuildServer=false \
  /nodeReuse:false \
  --verbosity quiet 2>&1) || {
  echo "$BUILD_OUTPUT" >&2
  exit 1
}

# Run tests with coverage for the entire solution (suppress output unless there's an error)
TEST_OUTPUT=$(dotnet test src/GraphlessDB.sln \
  --nodereuse:false \
  --collect:"XPlat Code Coverage" \
  --settings:"src/settings.runsettings" \
  --results-directory "$COVERAGE_DIR" \
  --verbosity quiet \
  --no-build 2>&1) || {
  echo "$TEST_OUTPUT" >&2
  exit 1
}

# Find all coverage.cobertura.xml files
COVERAGE_FILES=$(find "$COVERAGE_DIR" -name "coverage.cobertura.xml")

if [ -z "$COVERAGE_FILES" ]; then
  echo "Error: No coverage files found" >&2
  exit 1
fi

# Parse all coverage files and calculate total coverage
python3 << 'PYTHON_SCRIPT'
import xml.etree.ElementTree as ET
import json
import sys
import os
import glob

# Find all coverage files
coverage_dir = os.environ.get('COVERAGE_DIR', '.coverage')
coverage_files = glob.glob(f"{coverage_dir}/**/coverage.cobertura.xml", recursive=True)

if not coverage_files:
    print("Error: No coverage files found", file=sys.stderr)
    sys.exit(1)

total_lines_covered = 0
total_lines_valid = 0
total_branches_covered = 0
total_branches_valid = 0

# Parse each coverage file
for coverage_file in coverage_files:
    try:
        tree = ET.parse(coverage_file)
        root = tree.getroot()

        # Get the root coverage element which has line-rate and branch-rate attributes
        line_rate = float(root.get('line-rate', 0))
        branch_rate = float(root.get('branch-rate', 0))
        lines_covered = int(root.get('lines-covered', 0))
        lines_valid = int(root.get('lines-valid', 0))
        branches_covered = int(root.get('branches-covered', 0))
        branches_valid = int(root.get('branches-valid', 0))

        # Accumulate totals
        total_lines_covered += lines_covered
        total_lines_valid += lines_valid
        total_branches_covered += branches_covered
        total_branches_valid += branches_valid

    except Exception as e:
        print(f"Error parsing {coverage_file}: {e}", file=sys.stderr)
        sys.exit(1)

# Calculate percentages
if total_lines_valid > 0:
    line_coverage_percentage = round((total_lines_covered / total_lines_valid) * 100, 2)
else:
    line_coverage_percentage = 0

if total_branches_valid > 0:
    branch_coverage_percentage = round((total_branches_covered / total_branches_valid) * 100, 2)
else:
    branch_coverage_percentage = 0

# Output JSON
result = {
    "lineCoveragePercentage": line_coverage_percentage,
    "branchCoveragePercentage": branch_coverage_percentage
}

print(json.dumps(result))

PYTHON_SCRIPT
