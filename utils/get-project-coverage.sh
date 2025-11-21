#!/bin/bash

# Run code coverage for a single project's tests
# Outputs JSON array of files with low coverage: [{ name: "ClassName", path: "path/to/file", lines: { coverageRatio: 0.5, covered: 123, notCovered: 123 } }]

set -e

# Disable MSBuild node reuse to prevent hanging processes
export MSBUILDDISABLENODEREUSE=1

# Navigate to project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT"

# Parse arguments
PROJECT_PATH=""
LIMIT=5

if [ $# -eq 0 ]; then
  echo "Error: Project path is required" >&2
  echo "Usage: $0 <project-path> [limit]" >&2
  exit 1
fi

PROJECT_PATH="$1"
if [ $# -ge 2 ]; then
  LIMIT="$2"
fi

# Validate project path
FULL_PROJECT_PATH="$REPO_ROOT/src/$PROJECT_PATH"
if [ ! -f "$FULL_PROJECT_PATH" ]; then
  echo "Error: Project file not found at $FULL_PROJECT_PATH" >&2
  exit 1
fi

# Extract project name
PROJECT_NAME=$(basename "$PROJECT_PATH" .csproj)

# Check if this is a test project
if [[ "$PROJECT_NAME" == *.Tests ]]; then
  echo "Error: Cannot run coverage on a test project: $PROJECT_NAME" >&2
  exit 1
fi

# Find the corresponding test project
TEST_PROJECT_NAME="${PROJECT_NAME}.Tests"
TEST_PROJECT_PATH="src/${TEST_PROJECT_NAME}/${TEST_PROJECT_NAME}.csproj"

if [ ! -f "$REPO_ROOT/$TEST_PROJECT_PATH" ]; then
  echo "Error: Test project not found at $REPO_ROOT/$TEST_PROJECT_PATH" >&2
  exit 1
fi

# Create unique coverage directory
COVERAGE_DIR=".coverage/project-$(date +%s)-$$"
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

# Run tests with coverage for the specific test project (suppress output unless there's an error)
TEST_OUTPUT=$(dotnet test "$TEST_PROJECT_PATH" \
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

# Export variables for Python script
export COVERAGE_DIR
export LIMIT
export REPO_ROOT

# Parse all coverage files and extract per-file coverage data
python3 << 'PYTHON_SCRIPT'
import xml.etree.ElementTree as ET
import json
import sys
import os
import glob

# Get parameters from environment
coverage_dir = os.environ.get('COVERAGE_DIR', '.coverage')
limit = int(os.environ.get('LIMIT', 5))
repo_root = os.environ.get('REPO_ROOT', '')

# Find all coverage files
coverage_files = glob.glob(f"{coverage_dir}/**/coverage.cobertura.xml", recursive=True)

if not coverage_files:
    print("Error: No coverage files found", file=sys.stderr)
    sys.exit(1)

# Collect coverage data for all classes
class_coverage = {}

# Parse each coverage file
for coverage_file in coverage_files:
    try:
        tree = ET.parse(coverage_file)
        root = tree.getroot()

        # Get the source path from the XML
        sources = root.find('sources')
        source_prefix = ''
        if sources is not None:
            source_element = sources.find('source')
            if source_element is not None and source_element.text:
                source_prefix = source_element.text

        # Navigate through packages -> classes
        packages = root.find('packages')
        if packages is None:
            continue

        for package in packages.findall('package'):
            classes = package.find('classes')
            if classes is None:
                continue

            for cls in classes.findall('class'):
                filename = cls.get('filename', '')
                classname = cls.get('name', '')

                # Get line coverage stats from lines element
                lines_element = cls.find('lines')
                if lines_element is None:
                    continue

                lines = lines_element.findall('line')

                # Count covered and not covered lines
                covered = 0
                not_covered = 0

                for line in lines:
                    hits = int(line.get('hits', 0))
                    if hits > 0:
                        covered += 1
                    else:
                        not_covered += 1

                total_lines = covered + not_covered
                if total_lines == 0:
                    continue

                coverage_ratio = covered / total_lines

                # Construct full path from source prefix and filename
                # The filename in the XML is relative to the source prefix
                # The source prefix already ends with '/' so just concatenate
                if source_prefix:
                    # Normalize paths - source_prefix ends with /, filename is relative
                    full_path = source_prefix.rstrip('/') + '/' + filename
                else:
                    full_path = filename

                # Make path relative to repo root (add trailing slash to ensure proper matching)
                repo_root_with_slash = repo_root if repo_root.endswith('/') else repo_root + '/'

                if full_path.startswith(repo_root_with_slash):
                    rel_path = full_path[len(repo_root_with_slash):]
                else:
                    # Fallback - just use the filename part
                    rel_path = filename

                # Store or update coverage data (aggregate if same file appears in multiple coverage files)
                if rel_path in class_coverage:
                    existing = class_coverage[rel_path]
                    existing['covered'] += covered
                    existing['notCovered'] += not_covered
                    total = existing['covered'] + existing['notCovered']
                    existing['coverageRatio'] = existing['covered'] / total if total > 0 else 0
                else:
                    class_coverage[rel_path] = {
                        'name': classname,
                        'path': rel_path,
                        'coverageRatio': coverage_ratio,
                        'covered': covered,
                        'notCovered': not_covered
                    }

    except Exception as e:
        print(f"Error parsing {coverage_file}: {e}", file=sys.stderr)
        sys.exit(1)

# Filter out files with coverage >= 0.9
filtered_coverage = [
    item for item in class_coverage.values()
    if item['coverageRatio'] < 0.9
]

# Sort by coverage ratio (ascending - lowest coverage first)
sorted_coverage = sorted(filtered_coverage, key=lambda x: x['coverageRatio'])

# Limit the results
limited_coverage = sorted_coverage[:limit]

# Format output
result = []
for item in limited_coverage:
    result.append({
        'name': item['name'],
        'path': item['path'],
        'lines': {
            'coverageRatio': round(item['coverageRatio'], 4),
            'covered': item['covered'],
            'notCovered': item['notCovered']
        }
    })

print(json.dumps(result))

PYTHON_SCRIPT
