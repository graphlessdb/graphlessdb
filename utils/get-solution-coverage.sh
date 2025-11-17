#!/bin/bash

# Run code coverage for all unit tests in the solution
# Outputs JSON: { "lineCoveragePercentage": X, "branchCoveragePercentage": Y }

set -e

# Disable MSBuild node reuse to prevent hanging processes
export MSBUILDDISABLENODEREUSE=1

# Navigate to project root
cd "$(dirname "$0")/.."

# Create unique coverage directory
COVERAGE_DIR=".coverage/solution-$(date +%s)"
mkdir -p "$COVERAGE_DIR"

# Build the solution first
dotnet build src/GraphlessDB.sln --configuration Debug > /dev/null 2>&1

# Run tests with coverage
dotnet test src/GraphlessDB.sln \
  --collect:"XPlat Code Coverage" \
  --settings:"src/settings.runsettings" \
  --results-directory "$COVERAGE_DIR" \
  --verbosity quiet \
  --no-build \
  > /dev/null 2>&1

# Find the coverage.cobertura.xml file
COVERAGE_FILE=$(find "$COVERAGE_DIR" -name "coverage.cobertura.xml" | head -n 1)

if [ -z "$COVERAGE_FILE" ]; then
  echo "Error: Coverage file not found"
  rm -rf "$COVERAGE_DIR"
  exit 1
fi

# Extract coverage percentages using grep and awk
LINE_RATE=$(grep -o 'line-rate="[0-9.]*"' "$COVERAGE_FILE" | head -n 1 | grep -o '[0-9.]*')
BRANCH_RATE=$(grep -o 'branch-rate="[0-9.]*"' "$COVERAGE_FILE" | head -n 1 | grep -o '[0-9.]*')

# Convert to percentages (multiply by 100)
LINE_PERCENTAGE=$(echo "$LINE_RATE * 100" | bc -l | xargs printf "%.2f")
BRANCH_PERCENTAGE=$(echo "$BRANCH_RATE * 100" | bc -l | xargs printf "%.2f")

# Clean up coverage files
rm -rf "$COVERAGE_DIR"

# Output JSON
echo "{ \"lineCoveragePercentage\": $LINE_PERCENTAGE, \"branchCoveragePercentage\": $BRANCH_PERCENTAGE }"
