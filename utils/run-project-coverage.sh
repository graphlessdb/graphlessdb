#!/bin/bash

# Run code coverage for all unit tests within a single project
# Outputs JSON array of classes with low coverage (< 0.9)
# Format: [{ name: "ClassName", path: "path/to/file.cs", lines: { coverageRatio: 0.5, covered: 10, notCovered: 10 } }, ...]

set -e

# Disable MSBuild node reuse to prevent hanging processes
export MSBUILDDISABLENODEREUSE=1

# Navigate to project root
cd "$(dirname "$0")/.."

# Parameters
PROJECT_PATH=$1
LIMIT=${2:-5}

if [ -z "$PROJECT_PATH" ]; then
  echo "Error: Project path is required"
  echo "Usage: $0 <project_path> [limit]"
  exit 1
fi

# Create unique coverage directory
COVERAGE_DIR=".coverage/project-$(date +%s)-$$"
mkdir -p "$COVERAGE_DIR"

# Build the solution first
dotnet build src/GraphlessDB.sln --no-incremental -p:UseSharedCompilation=false -p:UseRazorBuildServer=false /nodeReuse:false --configuration Debug
# > /dev/null 2>&1

# Run tests with coverage for the specific project
dotnet test src/GraphlessDB.sln \
  --nodereuse:false \
  --collect:"XPlat Code Coverage" \
  --settings:"src/settings.runsettings" \
  --results-directory "$COVERAGE_DIR" \
  --verbosity quiet \
  --no-build \
  # > /dev/null 2>&1

# Find the coverage.cobertura.xml file
COVERAGE_FILE=$(find "$COVERAGE_DIR" -name "coverage.cobertura.xml" | head -n 1)

if [ -z "$COVERAGE_FILE" ]; then
  echo "Error: Coverage file not found"
  rm -rf "$COVERAGE_DIR"
  exit 1
fi

# Export variables for Python script
export COVERAGE_FILE
export PROJECT_PATH
export LIMIT

# Parse the coverage XML and extract class-level data
# We'll use python3 for easier XML parsing
python3 << 'PYTHON_SCRIPT'
import xml.etree.ElementTree as ET
import json
import sys
import os

# Read the coverage file path from environment
coverage_file = os.environ.get('COVERAGE_FILE')
project_path = os.environ.get('PROJECT_PATH')
limit = int(os.environ.get('LIMIT', 5))

# Parse the XML
tree = ET.parse(coverage_file)
root = tree.getroot()

# Extract project name from path
project_name = os.path.basename(project_path).replace('.csproj', '')

# Find all classes and their coverage
classes_data = []

# Navigate through packages -> classes
for package in root.findall('.//package'):
    for cls in package.findall('.//class'):
        filename = cls.get('filename', '')
        classname = cls.get('name', '')

        # Skip if not in the target project
        if project_name not in filename:
            continue

        # Skip compiler-generated classes (lambdas, async state machines, display classes, etc.)
        simple_classname = classname.split('.')[-1]
        if '<' in simple_classname or '>' in simple_classname:
            continue

        # Calculate coverage from lines
        covered = 0
        not_covered = 0

        for line in cls.findall('.//line'):
            hits = int(line.get('hits', 0))
            if hits > 0:
                covered += 1
            else:
                not_covered += 1

        total = covered + not_covered
        if total == 0:
            continue

        coverage_ratio = covered / total

        # Filter out items with coverage > 0.9
        if coverage_ratio > 0.9:
            continue

        # Get relative path from root
        root_dir = os.getcwd()

        # Handle absolute paths
        if filename.startswith(root_dir):
            relative_path = filename[len(root_dir)+1:]
        # Handle paths that already start with src/
        elif filename.startswith('src/'):
            relative_path = filename
        # Try to extract the src/... portion from the path
        elif 'src/' in filename:
            src_index = filename.index('src/')
            relative_path = filename[src_index:]
        # If no src/ in path, it might be a relative path from project, prepend src/
        else:
            # Path is like "GraphlessDB.DynamoDB/..." so prepend "src/"
            relative_path = 'src/' + filename

        classes_data.append({
            'name': simple_classname,
            'path': relative_path,
            'lines': {
                'coverageRatio': round(coverage_ratio, 4),
                'covered': covered,
                'notCovered': not_covered
            }
        })

# Sort by coverageRatio (ascending)
classes_data.sort(key=lambda x: x['lines']['coverageRatio'])

# Limit the results
classes_data = classes_data[:limit]

# Output JSON
print(json.dumps(classes_data, indent=2))

PYTHON_SCRIPT

# Clean up coverage files
# rm -rf "$COVERAGE_DIR"
