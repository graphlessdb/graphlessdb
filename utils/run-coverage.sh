#!/bin/bash

# Run code coverage for all GraphlessDB unit tests

set -e

echo "Running GraphlessDB Code Coverage..."
echo "===================================="

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Change to the src directory
cd "$PROJECT_ROOT/src"

# Create coverage directory
COVERAGE_DIR="$PROJECT_ROOT/coverage"
mkdir -p "$COVERAGE_DIR"

# Run tests with code coverage for GraphlessDB.Tests
echo ""
echo "Running tests with coverage for GraphlessDB.Tests..."
dotnet test GraphlessDB.Tests/GraphlessDB.Tests.csproj \
    --configuration Release \
    --collect:"XPlat Code Coverage" \
    --results-directory:"$COVERAGE_DIR/GraphlessDB.Tests"

# Run tests with code coverage for GraphlessDB.DynamoDB.Tests
echo ""
echo "Running tests with coverage for GraphlessDB.DynamoDB.Tests..."
dotnet test GraphlessDB.DynamoDB.Tests/GraphlessDB.DynamoDB.Tests.csproj \
    --configuration Release \
    --collect:"XPlat Code Coverage" \
    --results-directory:"$COVERAGE_DIR/GraphlessDB.DynamoDB.Tests"

echo ""
echo "===================================="
echo "Code coverage data collection complete!"
echo ""
echo "Coverage files generated in: $COVERAGE_DIR"
echo ""

# Find all coverage.cobertura.xml files
COVERAGE_FILES=$(find "$COVERAGE_DIR" -name "coverage.cobertura.xml")

if [ -z "$COVERAGE_FILES" ]; then
    echo "Warning: No coverage.cobertura.xml files found."
else
    echo "Coverage files:"
    echo "$COVERAGE_FILES"
    echo ""

    # Display basic coverage summary from each file
    for file in $COVERAGE_FILES; do
        echo "Summary from: $file"
        echo "----------------------------------------"

        # Extract line coverage percentage using grep and sed
        LINE_RATE=$(grep -o 'line-rate="[0-9.]*"' "$file" | head -1 | sed 's/line-rate="\([0-9.]*\)"/\1/')
        BRANCH_RATE=$(grep -o 'branch-rate="[0-9.]*"' "$file" | head -1 | sed 's/branch-rate="\([0-9.]*\)"/\1/')

        if [ -n "$LINE_RATE" ]; then
            LINE_PERCENT=$(awk "BEGIN {printf \"%.2f\", $LINE_RATE * 100}")
            echo "Line Coverage: ${LINE_PERCENT}%"
        fi

        if [ -n "$BRANCH_RATE" ]; then
            BRANCH_PERCENT=$(awk "BEGIN {printf \"%.2f\", $BRANCH_RATE * 100}")
            echo "Branch Coverage: ${BRANCH_PERCENT}%"
        fi

        echo ""
    done
fi

echo "To generate HTML reports, you can install and use reportgenerator:"
echo "  dotnet tool install -g dotnet-reportgenerator-globaltool"
echo "  reportgenerator -reports:\"$COVERAGE_DIR/**/coverage.cobertura.xml\" -targetdir:\"$COVERAGE_DIR/report\" -reporttypes:Html"
