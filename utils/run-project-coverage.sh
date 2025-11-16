#!/bin/zsh

# Run code coverage for a single project and return file-level coverage data
# Usage: ./run-project-coverage.sh <project-path> [limit]
# Arguments:
#   project-path: Full project filepath relative to root (e.g., "src/GraphlessDB.Tests/GraphlessDB.Tests.csproj")
#   limit: Optional, number of results to return (default: 5)
# Output: JSON array with format [{ name: "ClassName", path: "path/to/file", lines: {covered: 123, notCovered: 123} }]

set -e

# Get the script directory and repository root
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Parse arguments
if [ -z "$1" ]; then
    echo "Error: Project path is required" >&2
    echo "Usage: $0 <project-path> [limit]" >&2
    exit 1
fi

PROJECT_PATH="$1"
LIMIT="${2:-5}"

# Ensure we're on the main branch with latest source
cd "$REPO_ROOT"
git checkout main > /dev/null 2>&1
git pull > /dev/null 2>&1

# Verify project file exists
FULL_PROJECT_PATH="$REPO_ROOT/$PROJECT_PATH"
if [ ! -f "$FULL_PROJECT_PATH" ]; then
    echo "Error: Project file not found: $PROJECT_PATH" >&2
    exit 1
fi

# Build the entire solution first
cd "$REPO_ROOT/src"
dotnet build GraphlessDB.sln --configuration Release --verbosity:quiet --nologo > /dev/null 2>&1

# Create unique coverage directory
TIMESTAMP=$(date +%s)
UNIQUE_ID="project_$(basename "$PROJECT_PATH" .csproj)_${TIMESTAMP}_$$"
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

# Run tests with code coverage for the specific project
dotnet test "$FULL_PROJECT_PATH" \
    --configuration Release \
    --collect:"XPlat Code Coverage" \
    --results-directory:"$COVERAGE_DIR" \
    --settings:"$REPO_ROOT/src/settings.runsettings" \
    --verbosity:quiet \
    --nologo \
    --no-build \
    > /dev/null 2>&1

# Find the coverage XML file
COVERAGE_FILE=$(find "$COVERAGE_DIR" -name "coverage.cobertura.xml" | head -1)

if [ -z "$COVERAGE_FILE" ]; then
    echo "Error: No coverage file found" >&2
    exit 1
fi

# Parse the coverage XML and generate JSON output
python3 - "$COVERAGE_FILE" "$LIMIT" "$REPO_ROOT" <<'PYTHON_SCRIPT'
import sys
import xml.etree.ElementTree as ET
import json
import os

coverage_file = sys.argv[1]
limit = int(sys.argv[2])
repo_root = sys.argv[3]

# Parse the XML file
tree = ET.parse(coverage_file)
root = tree.getroot()

# Get source paths from the coverage file
source_paths = []
sources = root.findall('.//sources/source')
for source in sources:
    source_path = source.text
    if source_path:
        source_paths.append(source_path)

# Extract file-level coverage data
file_coverage = []

# Navigate through packages -> classes
packages = root.findall('.//package')
for package in packages:
    classes = package.findall('.//class')
    for cls in classes:
        filename = cls.get('filename', '')
        class_name = cls.get('name', '')

        # Get line coverage
        lines = cls.findall('.//line')
        covered = 0
        not_covered = 0

        for line in lines:
            hits = int(line.get('hits', '0'))
            if hits > 0:
                covered += 1
            else:
                not_covered += 1

        # Convert to absolute path if needed, then to relative
        absolute_path = filename

        # If filename is relative, try to combine it with source paths
        if not filename.startswith('/'):
            for source_path in source_paths:
                potential_path = os.path.join(source_path, filename)
                if os.path.exists(potential_path):
                    absolute_path = potential_path
                    break

        # Convert absolute path to relative path
        relative_path = filename
        if absolute_path.startswith(repo_root):
            relative_path = os.path.relpath(absolute_path, repo_root)
        elif absolute_path.startswith('/'):
            # Try to make it relative anyway
            try:
                relative_path = os.path.relpath(absolute_path, repo_root)
            except:
                relative_path = filename

        # Normalize the path separators
        relative_path = relative_path.replace('\\', '/')

        # Extract just the class name (without namespace)
        simple_class_name = class_name.split('.')[-1] if class_name else os.path.basename(filename).replace('.cs', '')

        file_coverage.append({
            'name': simple_class_name,
            'path': relative_path,
            'lines': {
                'covered': covered,
                'notCovered': not_covered
            }
        })

# Sort by notCovered (descending)
file_coverage.sort(key=lambda x: x['lines']['notCovered'], reverse=True)

# Limit the results
file_coverage = file_coverage[:limit]

# Output JSON
print(json.dumps(file_coverage, indent=2))

PYTHON_SCRIPT
