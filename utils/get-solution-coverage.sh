#!/bin/zsh

# Run code coverage for all unit tests in the solution
# Usage: ./get-solution-coverage.sh
# Output: JSON with lineCoveragePercentage and branchCoveragePercentage

set -e

# Get the script directory and repository root
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Create unique coverage directory
TIMESTAMP=$(date +%s)
UNIQUE_ID="solution_${TIMESTAMP}_$$"
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

# Run tests with code coverage for the entire solution
cd "$REPO_ROOT/src"
dotnet test GraphlessDB.sln \
    --configuration Release \
    --collect:"XPlat Code Coverage" \
    --results-directory:"$COVERAGE_DIR" \
    --settings:settings.runsettings \
    --verbosity:quiet \
    --nologo \
    > /dev/null 2>&1

# Find all coverage XML files and merge them if multiple exist
COVERAGE_FILES=$(find "$COVERAGE_DIR" -name "coverage.cobertura.xml")

if [ -z "$COVERAGE_FILES" ]; then
    echo "Error: No coverage files found" >&2
    exit 1
fi

# Count coverage files
COVERAGE_FILE_COUNT=$(echo "$COVERAGE_FILES" | wc -l | tr -d ' ')

if [ "$COVERAGE_FILE_COUNT" -eq 1 ]; then
    # Single coverage file - use it directly
    COVERAGE_FILE="$COVERAGE_FILES"
else
    # Multiple coverage files - merge them
    # For now, we'll parse all files and aggregate the results
    COVERAGE_FILE="$COVERAGE_FILES"
fi

# Parse the coverage XML file(s) and generate JSON output using Python
RESULT=$(python3 - "$COVERAGE_FILE" <<'PYTHON_SCRIPT'
import sys
import xml.etree.ElementTree as ET
import json

coverage_files = sys.argv[1].strip().split('\n')

total_lines_valid = 0
total_lines_covered = 0
total_branches_valid = 0
total_branches_covered = 0

for coverage_file in coverage_files:
    # Parse the XML file
    tree = ET.parse(coverage_file)
    root = tree.getroot()
    
    # Get line and branch coverage from the root element
    line_rate = root.get('line-rate')
    branch_rate = root.get('branch-rate')
    lines_valid = root.get('lines-valid')
    lines_covered = root.get('lines-covered')
    branches_valid = root.get('branches-valid')
    branches_covered = root.get('branches-covered')
    
    if lines_valid:
        total_lines_valid += int(lines_valid)
    if lines_covered:
        total_lines_covered += int(lines_covered)
    if branches_valid:
        total_branches_valid += int(branches_valid)
    if branches_covered:
        total_branches_covered += int(branches_covered)

# Calculate percentages
line_coverage_pct = 0
if total_lines_valid > 0:
    line_coverage_pct = round((total_lines_covered / total_lines_valid) * 100, 2)

branch_coverage_pct = 0
if total_branches_valid > 0:
    branch_coverage_pct = round((total_branches_covered / total_branches_valid) * 100, 2)

# Output JSON
result = {
    'lineCoveragePercentage': line_coverage_pct,
    'branchCoveragePercentage': branch_coverage_pct
}

print(json.dumps(result))

PYTHON_SCRIPT
)

# Output the result
echo "$RESULT"
