#!/bin/bash

# Run all unit tests in the solution

set -e

# Navigate to the source directory
cd "$(dirname "$0")/../src"

# Run all tests
dotnet test GraphlessDB.sln --nodereuse:false --verbosity minimal
