#!/bin/bash

# Calculate CRAP scores for all types and functions
# Outputs JSON array of functions ordered by CRAP score (worst first):
# [{ typeName: "", methodName: "", filePath: "", crapScore: 0 }]

set -e

# Disable MSBuild node reuse to prevent hanging processes
export MSBUILDDISABLENODEREUSE=1

# Navigate to project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT"

# Parse arguments
JSON_ONLY=false
LIMIT=10

for arg in "$@"; do
  case $arg in
    --json)
      JSON_ONLY=true
      shift
      ;;
    *)
      # Unknown argument
      ;;
  esac
done

# Output start message unless --json flag is set
if [ "$JSON_ONLY" = false ]; then
  echo "Calculating CRAP scores..." >&2
fi

# Create unique coverage directory
COVERAGE_DIR=".coverage/crap-$(date +%s)-$$"
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

# Run tests with coverage for all test projects (suppress output unless there's an error)
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

# Export variables for Python script
export COVERAGE_DIR
export LIMIT
export REPO_ROOT

# Parse all coverage files and calculate CRAP scores
RESULT=$(python3 << 'PYTHON_SCRIPT'
import xml.etree.ElementTree as ET
import json
import sys
import os
import glob

# Get parameters from environment
coverage_dir = os.environ.get('COVERAGE_DIR', '.coverage')
limit = int(os.environ.get('LIMIT', 10))
repo_root = os.environ.get('REPO_ROOT', '')

# Find all coverage files
coverage_files = glob.glob(f"{coverage_dir}/**/coverage.cobertura.xml", recursive=True)

if not coverage_files:
    print("Error: No coverage files found", file=sys.stderr)
    sys.exit(1)

# Collect coverage data for all methods across all files
# Key: (file_path, type_name, method_name)
# Value: { covered_lines, total_lines, complexity }
method_coverage = {}

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

                # Construct full path from source prefix and filename
                if source_prefix:
                    full_path = source_prefix.rstrip('/') + '/' + filename
                else:
                    full_path = filename

                # Make path relative to repo root
                repo_root_with_slash = repo_root if repo_root.endswith('/') else repo_root + '/'

                if full_path.startswith(repo_root_with_slash):
                    rel_path = full_path[len(repo_root_with_slash):]
                else:
                    rel_path = filename

                # Get methods
                methods = cls.find('methods')
                if methods is None:
                    continue

                for method in methods.findall('method'):
                    method_name = method.get('name', '')

                    # Skip compiler-generated methods (like lambda display classes)
                    if 'c__DisplayClass' in method_name or '<' in method_name or '>' in method_name:
                        continue

                    # Get complexity (cyclomatic complexity)
                    # Cobertura reports use complexity attribute on method
                    complexity_str = method.get('complexity', '1')
                    try:
                        complexity = int(complexity_str)
                    except ValueError:
                        complexity = 1

                    # Get line coverage for this method
                    lines_element = method.find('lines')
                    if lines_element is None:
                        continue

                    lines = lines_element.findall('line')

                    covered = 0
                    total = 0

                    for line in lines:
                        hits = int(line.get('hits', 0))
                        total += 1
                        if hits > 0:
                            covered += 1

                    if total == 0:
                        continue

                    # Create unique key for this method
                    key = (rel_path, classname, method_name)

                    # If we've seen this method before (from different coverage run),
                    # keep the one with better coverage
                    if key in method_coverage:
                        existing = method_coverage[key]
                        existing_coverage_ratio = existing['covered'] / existing['total'] if existing['total'] > 0 else 0
                        new_coverage_ratio = covered / total if total > 0 else 0

                        if new_coverage_ratio > existing_coverage_ratio:
                            method_coverage[key] = {
                                'covered': covered,
                                'total': total,
                                'complexity': complexity
                            }
                    else:
                        method_coverage[key] = {
                            'covered': covered,
                            'total': total,
                            'complexity': complexity
                        }

    except Exception as e:
        print(f"Error parsing {coverage_file}: {e}", file=sys.stderr)
        sys.exit(1)

# Calculate CRAP scores
# CRAP = complexity^2 * (1 - coverage)^3 + complexity
crap_results = []

for (file_path, type_name, method_name), data in method_coverage.items():
    covered = data['covered']
    total = data['total']
    complexity = data['complexity']

    coverage_ratio = covered / total if total > 0 else 0

    # CRAP formula
    crap_score = (complexity ** 2) * ((1 - coverage_ratio) ** 3) + complexity

    crap_results.append({
        'typeName': type_name,
        'methodName': method_name,
        'filePath': file_path,
        'crapScore': round(crap_score, 2),
        'complexity': complexity,
        'coverageRatio': round(coverage_ratio, 4)
    })

# Sort by CRAP score (descending - worst first)
sorted_results = sorted(crap_results, key=lambda x: x['crapScore'], reverse=True)

# Limit the results
limited_results = sorted_results[:limit]

# Format output - remove debug fields
output = []
for item in limited_results:
    output.append({
        'typeName': item['typeName'],
        'methodName': item['methodName'],
        'filePath': item['filePath'],
        'crapScore': item['crapScore']
    })

print(json.dumps(output))

PYTHON_SCRIPT
)

# Check if Python script succeeded
if [ $? -ne 0 ]; then
  exit 1
fi

# Output finish message unless --json flag is set
if [ "$JSON_ONLY" = false ]; then
  echo "CRAP score calculation complete." >&2
fi

# Output the JSON result
echo "$RESULT"
