#!/bin/bash

# Run all unit tests for GraphlessDB

set -e

echo "Running GraphlessDB Unit Tests..."
echo "================================="

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Change to the src directory
cd "$PROJECT_ROOT/src"

# Run tests for GraphlessDB.Tests
echo ""
echo "Running GraphlessDB.Tests..."
dotnet test GraphlessDB.Tests/GraphlessDB.Tests.csproj --configuration Release

# Run tests for GraphlessDB.DynamoDB.Tests
echo ""
echo "Running GraphlessDB.DynamoDB.Tests..."
dotnet test GraphlessDB.DynamoDB.Tests/GraphlessDB.DynamoDB.Tests.csproj --configuration Release

echo ""
echo "================================="
echo "All tests completed successfully!"
