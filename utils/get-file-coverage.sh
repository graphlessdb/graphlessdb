#!/bin/bash

# Get code coverage for a specific file
# Usage: ./get-file-coverage.sh <file-path>
# Example: ./get-file-coverage.sh ./src/GraphlessDB.DynamoDB/DynamoDB.Transactions.Storage/VersionedItemStore.cs

set -e

# Disable MSBuild node reuse to prevent hanging processes
export MSBUILDDISABLENODEREUSE=1

# Navigate to project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT"

# Parse arguments
if [ $# -eq 0 ]; then
  echo "Error: File path is required" >&2
  echo "Usage: $0 <file-path>" >&2
  exit 1
fi

FILE_PATH="$1"

# Remove leading ./ if present
FILE_PATH="${FILE_PATH#./}"

# Validate file exists
if [ ! -f "$REPO_ROOT/$FILE_PATH" ]; then
  echo "Error: File not found at $REPO_ROOT/$FILE_PATH" >&2
  exit 1
fi

# Extract class name from file path (remove .cs extension and get basename)
CLASS_NAME=$(basename "$FILE_PATH" .cs)

# Determine the project name from the file path
# Expected format: src/<ProjectName>/<optionalsubdirs>/<ClassName>.cs
if [[ ! "$FILE_PATH" =~ ^src/([^/]+)/ ]]; then
  echo "Error: File path must be in format src/<ProjectName>/.../<ClassName>.cs" >&2
  exit 1
fi

PROJECT_NAME="${BASH_REMATCH[1]}"

# Check if this is a test project
if [[ "$PROJECT_NAME" == *.Tests ]]; then
  echo "Error: Cannot run coverage on a test project file: $FILE_PATH" >&2
  exit 1
fi

# Find the corresponding test project
TEST_PROJECT_NAME="${PROJECT_NAME}.Tests"
TEST_PROJECT_DIR="$REPO_ROOT/src/$TEST_PROJECT_NAME"

if [ ! -d "$TEST_PROJECT_DIR" ]; then
  echo "{\"file\":\"$FILE_PATH\",\"error\":\"No test project found\",\"message\":\"Test project $TEST_PROJECT_NAME does not exist\"}"
  exit 0
fi

# Find the test file for this class
TEST_FILE=$(find "$TEST_PROJECT_DIR" -name "${CLASS_NAME}Tests.cs" -type f | head -1)

if [ -z "$TEST_FILE" ]; then
  echo "{\"file\":\"$FILE_PATH\",\"error\":\"No test file found\",\"message\":\"No test file named ${CLASS_NAME}Tests.cs found in $TEST_PROJECT_NAME\"}"
  exit 0
fi

# Extract the test class fully qualified name
# We'll use the namespace from the test file
TEST_CLASS_FQN=$(grep -E "^\s*namespace\s+" "$TEST_FILE" | head -1 | sed -E 's/^\s*namespace\s+([^;{]+).*/\1/')
if [ -n "$TEST_CLASS_FQN" ]; then
  TEST_CLASS_FQN="${TEST_CLASS_FQN}.${CLASS_NAME}Tests"
else
  TEST_CLASS_FQN="${CLASS_NAME}Tests"
fi

# Create unique coverage directory
COVERAGE_DIR=".coverage/file-$(date +%s)-$$"
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

# Run tests with coverage for the specific test class (suppress output unless there's an error)
TEST_PROJECT_PATH="src/${TEST_PROJECT_NAME}/${TEST_PROJECT_NAME}.csproj"
TEST_OUTPUT=$(dotnet test "$TEST_PROJECT_PATH" \
  --nodereuse:false \
  --collect:"XPlat Code Coverage" \
  --settings:"src/settings.runsettings" \
  --results-directory "$COVERAGE_DIR" \
  --filter:"FullyQualifiedName~$TEST_CLASS_FQN" \
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
export REPO_ROOT
export FILE_PATH
export PROJECT_NAME
export CLASS_NAME

# Parse coverage files and extract detailed coverage data
python3 << 'PYTHON_SCRIPT'
import xml.etree.ElementTree as ET
import json
import sys
import os
import glob
import re

# Get parameters from environment
coverage_dir = os.environ.get('COVERAGE_DIR', '.coverage')
repo_root = os.environ.get('REPO_ROOT', '')
target_file_path = os.environ.get('FILE_PATH', '')
project_name = os.environ.get('PROJECT_NAME', '')
class_name = os.environ.get('CLASS_NAME', '')

# Find all coverage files
coverage_files = glob.glob(f"{coverage_dir}/**/coverage.cobertura.xml", recursive=True)

if not coverage_files:
    print(json.dumps({
        "file": target_file_path,
        "error": "No coverage data",
        "message": "No coverage files generated"
    }))
    sys.exit(0)

# Normalize target file path for matching
target_file_normalized = target_file_path.replace('\\', '/')

# Helper function to normalize file paths for comparison
def normalize_path(path):
    return path.replace('\\', '/').rstrip('/')

# Helper function to check if a filename matches the target
def matches_target(filename, source_prefix=''):
    # Construct possible full paths
    if source_prefix:
        full_path = source_prefix.rstrip('/') + '/' + filename
    else:
        full_path = filename

    # Normalize paths
    full_path = normalize_path(full_path)
    target_norm = normalize_path(target_file_normalized)

    # Check various matching scenarios:
    # 1. Exact match
    if full_path == target_norm:
        return True

    # 2. Full path ends with target (target might be relative)
    if full_path.endswith('/' + target_norm):
        return True

    # 3. Full path contains the target after repo_root
    repo_root_norm = normalize_path(repo_root)
    if repo_root_norm and full_path.startswith(repo_root_norm):
        rel_path = full_path[len(repo_root_norm):].lstrip('/')
        if rel_path == target_norm:
            return True

    # 4. Just filename match as last resort
    target_filename = target_norm.split('/')[-1]
    full_filename = full_path.split('/')[-1]
    if target_filename == full_filename:
        return True

    return False

# Collect coverage data per file across all coverage runs
# For duplicate runs, keep the one with maximum coverage
file_coverage_runs = []

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

        # Collect all classes for the target file in this coverage run
        # This includes both the main class and compiler-generated async state machine classes
        target_file_data = {
            'classes': [],
            'source_prefix': source_prefix
        }

        for package in packages.findall('package'):
            classes = package.find('classes')
            if classes is None:
                continue

            for cls in classes.findall('class'):
                filename = cls.get('filename', '')
                classname = cls.get('name', '')

                # Check if this class belongs to the target file
                # Either it matches the filename directly, or it's a compiler-generated class
                is_target_file_match = matches_target(filename, source_prefix)

                # Also check if it's a compiler-generated class for our target class
                # These have names like: TargetClass/<MethodName>d__XX or TargetClass/<>c__DisplayClassXX_XX
                is_compiler_generated = False
                if class_name and '/' in classname:
                    # Extract the base class name before the '/'
                    base_class = classname.split('/')[0].split('.')[-1]  # Get last part after last dot
                    if base_class == class_name:
                        is_compiler_generated = True

                if not is_target_file_match and not is_compiler_generated:
                    continue

                # For compiler-generated classes, still validate the filename if available
                if is_compiler_generated and filename:
                    if not matches_target(filename, source_prefix):
                        continue

                # Also filter to only include files from the project being tested
                # Construct full path
                if source_prefix:
                    full_path = source_prefix.rstrip('/') + '/' + filename if filename else ''
                else:
                    full_path = filename

                # Make path relative to repo root
                if full_path:
                    repo_root_with_slash = repo_root if repo_root.endswith('/') else repo_root + '/'
                    if full_path.startswith(repo_root_with_slash):
                        rel_path = full_path[len(repo_root_with_slash):]
                    else:
                        rel_path = filename

                    # Filter: only include files from the project being tested
                    if project_name:
                        expected_prefix = f"src/{project_name}/"
                        if not rel_path.startswith(expected_prefix):
                            continue

                # Extract methods and lines
                methods_element = cls.find('methods')
                methods_data = []

                if methods_element is not None:
                    for method in methods_element.findall('method'):
                        method_name = method.get('name', '')

                        # For async state machine classes, extract the original method name from class name
                        # e.g., "Class/<MethodName>d__7" -> "MethodName"
                        actual_method_name = method_name
                        if '/' in classname and method_name == 'MoveNext':
                            # Extract method name from class name
                            # Pattern: <MethodName>d__XX
                            match = re.search(r'<([^>]+)>d__\d+', classname)
                            if match:
                                actual_method_name = match.group(1)

                        # Get complexity
                        complexity_str = method.get('complexity', '1')
                        try:
                            complexity = int(complexity_str)
                        except ValueError:
                            complexity = 1

                        # Get line coverage for this method
                        lines_element = method.find('lines')
                        method_lines = []

                        if lines_element is not None:
                            for line in lines_element.findall('line'):
                                line_number = int(line.get('number', 0))
                                hits = int(line.get('hits', 0))
                                branch = line.get('branch', 'false') == 'true'

                                # Determine line state
                                if hits > 0:
                                    state = 'COVERED'
                                else:
                                    state = 'UNCOVERED'

                                method_lines.append({
                                    'lineNumber': line_number,
                                    'hits': hits,
                                    'branch': branch,
                                    'state': state
                                })

                        methods_data.append({
                            'name': actual_method_name,
                            'complexity': complexity,
                            'lines': method_lines
                        })

                # Also get all lines from the class level
                lines_element = cls.find('lines')
                class_lines = []

                if lines_element is not None:
                    for line in lines_element.findall('line'):
                        line_number = int(line.get('number', 0))
                        hits = int(line.get('hits', 0))

                        if hits > 0:
                            state = 'COVERED'
                        else:
                            state = 'UNCOVERED'

                        class_lines.append({
                            'lineNumber': line_number,
                            'hits': hits,
                            'state': state
                        })

                target_file_data['classes'].append({
                    'name': classname,
                    'methods': methods_data,
                    'lines': class_lines
                })

        if target_file_data['classes']:
            file_coverage_runs.append(target_file_data)

    except Exception as e:
        print(json.dumps({
            "file": target_file_path,
            "error": "Coverage parsing error",
            "message": str(e)
        }), file=sys.stderr)
        sys.exit(1)

# If no coverage data found for the target file
if not file_coverage_runs:
    print(json.dumps({
        "file": target_file_path,
        "error": "No coverage data for file",
        "message": "File was not included in coverage report"
    }))
    sys.exit(0)

# Aggregate coverage data across all runs (taking maximum coverage)
# First, merge all classes across runs
all_classes = []
for run in file_coverage_runs:
    all_classes.extend(run['classes'])

# Aggregate methods across all classes
# Key: method name, Value: method data with lines
methods_aggregated = {}

# Also track all unique line numbers
all_line_numbers = set()

for cls in all_classes:
    for method in cls['methods']:
        method_name = method['name']

        # Track line numbers
        for line in method['lines']:
            all_line_numbers.add(line['lineNumber'])

        if method_name in methods_aggregated:
            # Merge lines, taking maximum hits
            existing_lines = {l['lineNumber']: l for l in methods_aggregated[method_name]['lines']}
            for line in method['lines']:
                ln = line['lineNumber']
                if ln in existing_lines:
                    if line['hits'] > existing_lines[ln]['hits']:
                        existing_lines[ln] = line
                else:
                    existing_lines[ln] = line

            methods_aggregated[method_name]['lines'] = list(existing_lines.values())
            # Update complexity to max
            methods_aggregated[method_name]['complexity'] = max(
                methods_aggregated[method_name]['complexity'],
                method['complexity']
            )
        else:
            methods_aggregated[method_name] = {
                'name': method_name,
                'complexity': method['complexity'],
                'lines': method['lines']
            }

# Also aggregate all lines at class level to ensure we have complete coverage
all_lines_aggregated = {}
for cls in all_classes:
    for line in cls['lines']:
        ln = line['lineNumber']
        if ln in all_lines_aggregated:
            if line['hits'] > all_lines_aggregated[ln]['hits']:
                all_lines_aggregated[ln] = line
        else:
            all_lines_aggregated[ln] = line

# Read the source file to determine uncoverable lines
# Lines that are not in the coverage report are considered UNCOVERABLE
source_file_path = os.path.join(repo_root, target_file_path)
total_source_lines = 0

try:
    with open(source_file_path, 'r', encoding='utf-8') as f:
        total_source_lines = len(f.readlines())
except:
    total_source_lines = max(all_line_numbers) if all_line_numbers else 0

# Build the final output
# Calculate summary metrics (including all code, even compiler-generated)
# But only count complexity for non-compiler-generated methods
total_covered = 0
total_uncovered = 0
total_coverable = 0
total_complexity = 0

for method_name, method_data in methods_aggregated.items():
    covered = sum(1 for l in method_data['lines'] if l['state'] == 'COVERED')
    uncovered = sum(1 for l in method_data['lines'] if l['state'] == 'UNCOVERED')
    total_covered += covered
    total_uncovered += uncovered

    # Only add complexity for non-compiler-generated methods
    if not ('c__DisplayClass' in method_name or ('<' in method_name and '>' in method_name) or method_name == 'MoveNext'):
        total_complexity += method_data['complexity']

total_coverable = total_covered + total_uncovered
coverage_percentage = (total_covered / total_coverable * 100) if total_coverable > 0 else 0

# Calculate CRAP score for summary
# CRAP = complexity^2 * (1 - coverage)^3 + complexity
coverage_ratio = total_covered / total_coverable if total_coverable > 0 else 0
crap_score = (total_complexity ** 2) * ((1 - coverage_ratio) ** 3) + total_complexity

# Build methods and properties output
methods_output = []

for method_name, method_data in methods_aggregated.items():
    # Skip compiler-generated methods
    # This includes lambda display classes and raw MoveNext methods that weren't mapped to async methods
    if 'c__DisplayClass' in method_name or ('<' in method_name and '>' in method_name) or method_name == 'MoveNext':
        continue

    method_lines = sorted(method_data['lines'], key=lambda x: x['lineNumber'])

    method_covered = sum(1 for l in method_lines if l['state'] == 'COVERED')
    method_uncovered = sum(1 for l in method_lines if l['state'] == 'UNCOVERED')
    method_coverable = method_covered + method_uncovered
    method_coverage_percentage = (method_covered / method_coverable * 100) if method_coverable > 0 else 0

    # Calculate method CRAP score
    method_coverage_ratio = method_covered / method_coverable if method_coverable > 0 else 0
    method_crap = (method_data['complexity'] ** 2) * ((1 - method_coverage_ratio) ** 3) + method_data['complexity']

    methods_output.append({
        'name': method_name,
        'lineCoverage': {
            'covered': method_covered,
            'uncovered': method_uncovered,
            'coverable': method_coverable,
            'coverableAndUncoverable': method_coverable,  # Same as coverable for now
            'coveragePercentage': round(method_coverage_percentage, 2),
            'cyclomaticComplexity': method_data['complexity'],
            'crapScore': round(method_crap, 2)
        },
        'lines': [
            {
                'lineNumber': l['lineNumber'],
                'state': l['state']
            }
            for l in method_lines
        ]
    })

# Sort methods by line number of first line
methods_output.sort(key=lambda m: m['lines'][0]['lineNumber'] if m['lines'] else 0)

# Build final output
result = {
    'file': target_file_path,
    'summary': {
        'lineCoverage': {
            'covered': total_covered,
            'uncovered': total_uncovered,
            'coverable': total_coverable,
            'coverableAndUncoverable': total_source_lines,
            'coveragePercentage': round(coverage_percentage, 2),
            'cyclomaticComplexity': total_complexity,
            'crapScore': round(crap_score, 2)
        }
    },
    'methodsAndProperties': methods_output
}

# Output formatted JSON
print(json.dumps(result, indent=2))

PYTHON_SCRIPT
