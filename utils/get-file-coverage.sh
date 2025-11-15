#!/bin/bash

# Run code coverage for a specific file and return JSON with coverage details

set -e

# Check if file name is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <file-name>" >&2
    echo "Example: $0 Connection.cs" >&2
    exit 1
fi

TARGET_FILE="$1"

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Change to the src directory
cd "$PROJECT_ROOT/src"

# Check if the input is a class name (contains dots) or a file name
if [[ "$TARGET_FILE" == *.*.* ]]; then
    # It's a fully qualified class name like GraphlessDB.Graph.Services.Internal.ClassName
    # Extract the class name (last part after the last dot)
    CLASS_PART="${TARGET_FILE##*.}"
    TARGET_FILE="${CLASS_PART}.cs"
fi

# Extract the base name without extension
BASE_NAME="${TARGET_FILE%.cs}"

# Find the target file in the source directories
TARGET_PATH=$(find . -name "$TARGET_FILE" -type f ! -path "*/bin/*" ! -path "*/obj/*" ! -path "*/TestResults/*" ! -path "*Tests*" | head -1)

if [ -z "$TARGET_PATH" ]; then
    echo "{\"error\": \"Target file not found: $TARGET_FILE\"}"
    exit 1
fi

# Get the directory containing the target file
TARGET_DIR=$(dirname "$TARGET_PATH")

# Extract class name from the file (namespace.classname format)
# First, try to get the namespace and class name from the file
NAMESPACE=$(grep -E "^namespace " "$TARGET_PATH" | head -1 | sed 's/namespace //' | sed 's/[; ]//g')
CLASS_NAME=$(grep -E "^\s*(public|internal|private|protected)?\s*(static|abstract|sealed)?\s*(class|struct|interface|enum|record)\s+[A-Za-z0-9_<>]+" "$TARGET_PATH" | head -1 | sed -E 's/.*\s(class|struct|interface|enum|record)\s+([A-Za-z0-9_<>]+).*/\2/')

# If we found both namespace and class name, create the full class identifier
if [ -n "$NAMESPACE" ] && [ -n "$CLASS_NAME" ]; then
    FULL_CLASS_NAME="$NAMESPACE.$CLASS_NAME"
else
    # Fallback: try to extract from path
    FULL_CLASS_NAME=""
fi

# Look for potential test files
# Common patterns: <ClassName>Tests.cs, <ClassName>Test.cs
TEST_FILE=""
TEST_PATH=""

# Search patterns
for pattern in "${BASE_NAME}Tests.cs" "${BASE_NAME}Test.cs" "${BASE_NAME}.Tests.cs"; do
    TEST_PATH=$(find . -name "$pattern" -type f ! -path "*/bin/*" ! -path "*/obj/*" ! -path "*/TestResults/*" | head -1)
    if [ -n "$TEST_PATH" ]; then
        TEST_FILE=$(basename "$TEST_PATH")
        break
    fi
done

# If no test file found, return early with null test
if [ -z "$TEST_FILE" ]; then
    export TARGET_FILE
    python3 <<'EOF'
import json
import os

target_file = os.environ.get('TARGET_FILE', '')

result = {
    'file': target_file,
    'test': None,
    'coverageOutput': 'No test file found'
}

print(json.dumps(result, indent=2))
EOF
    exit 0
fi

# Get the test project path
TEST_PROJECT_DIR=$(dirname "$TEST_PATH")
TEST_PROJECT_NAME=$(basename "$TEST_PROJECT_DIR")

# Find the test project file
TEST_PROJECT_FILE=$(find "$TEST_PROJECT_DIR" -maxdepth 1 -name "*.csproj" | head -1)

if [ -z "$TEST_PROJECT_FILE" ]; then
    # If not in a project directory directly, look up one level
    TEST_PROJECT_DIR=$(dirname "$TEST_PROJECT_DIR")
    TEST_PROJECT_NAME=$(basename "$TEST_PROJECT_DIR")
    TEST_PROJECT_FILE=$(find "$TEST_PROJECT_DIR" -maxdepth 1 -name "*.csproj" | head -1)
fi

if [ -z "$TEST_PROJECT_FILE" ]; then
    export TARGET_FILE TEST_FILE
    python3 <<'EOF'
import json
import os

target_file = os.environ.get('TARGET_FILE', '')
test_file = os.environ.get('TEST_FILE', '')

result = {
    'file': target_file,
    'test': test_file,
    'coverageOutput': 'Test project file not found'
}

print(json.dumps(result, indent=2))
EOF
    exit 0
fi

# Create coverage directory
COVERAGE_DIR="$PROJECT_ROOT/coverage/$TEST_PROJECT_NAME"
mkdir -p "$COVERAGE_DIR"

# Run tests with code coverage using a filter for the specific test file
FILTER="FullyQualifiedName~${BASE_NAME}"

dotnet test "$TEST_PROJECT_FILE" \
    --configuration Release \
    --collect:"XPlat Code Coverage" \
    --results-directory:"$COVERAGE_DIR" \
    --filter "$FILTER" \
    --verbosity quiet > /dev/null 2>&1 || true

# Find the coverage.cobertura.xml file
COVERAGE_FILE=$(find "$COVERAGE_DIR" -name "coverage.cobertura.xml" -type f | sort -r | head -1)

if [ -z "$COVERAGE_FILE" ]; then
    export TARGET_FILE TEST_FILE
    python3 <<'EOF'
import json
import os

target_file = os.environ.get('TARGET_FILE', '')
test_file = os.environ.get('TEST_FILE', '')

result = {
    'file': target_file,
    'test': test_file,
    'coverageOutput': 'No coverage data generated'
}

print(json.dumps(result, indent=2))
EOF
    exit 0
fi

# Export variables for Python script
export TARGET_FILE
export TEST_FILE
export COVERAGE_FILE
export FULL_CLASS_NAME
export BASE_NAME

# Parse the coverage XML and generate JSON output
python3 <<'EOF'
import xml.etree.ElementTree as ET
import json
import sys
import os

target_file = os.environ.get('TARGET_FILE', '')
test_file = os.environ.get('TEST_FILE', '')
coverage_file = os.environ.get('COVERAGE_FILE', '')
full_class_name = os.environ.get('FULL_CLASS_NAME', '')
base_name = os.environ.get('BASE_NAME', '')

try:
    tree = ET.parse(coverage_file)
    root = tree.getroot()

    # Look for the specific class in the coverage report
    target_class = None

    # Try to find by full class name first
    if full_class_name:
        for class_elem in root.findall('.//class'):
            class_name = class_elem.get('name', '')
            if full_class_name in class_name or class_name.endswith('.' + full_class_name):
                target_class = class_elem
                break

    # If not found, try to find by filename
    if not target_class:
        for class_elem in root.findall('.//class'):
            filename = class_elem.get('filename', '')
            if target_file in filename or filename.endswith(target_file):
                target_class = class_elem
                break

    coverage_output = {}

    if target_class:
        class_name = target_class.get('name', '')
        line_rate = target_class.get('line-rate', '0')
        branch_rate = target_class.get('branch-rate', '0')

        # Count covered and not covered lines
        covered = 0
        not_covered = 0

        for line in target_class.findall('.//lines/line'):
            hits = int(line.get('hits', '0'))
            if hits > 0:
                covered += 1
            else:
                not_covered += 1

        total = covered + not_covered
        coverage_percentage = (covered / total * 100) if total > 0 else 0

        coverage_output = {
            'className': class_name,
            'lineRate': float(line_rate),
            'branchRate': float(branch_rate),
            'lines': {
                'covered': covered,
                'notCovered': not_covered,
                'total': total,
                'percentage': round(coverage_percentage, 2)
            }
        }
    else:
        # Class not found in coverage report, might mean 0% coverage or file not tested
        coverage_output = {
            'className': full_class_name if full_class_name else base_name,
            'lineRate': 0.0,
            'branchRate': 0.0,
            'lines': {
                'covered': 0,
                'notCovered': 0,
                'total': 0,
                'percentage': 0.0
            },
            'note': 'Class not found in coverage report - may not have been tested or may have no executable lines'
        }

    result = {
        'file': target_file,
        'test': test_file,
        'coverageOutput': coverage_output
    }

    print(json.dumps(result, indent=2))

except Exception as e:
    result = {
        'file': target_file,
        'test': test_file,
        'coverageOutput': {
            'error': str(e)
        }
    }
    print(json.dumps(result, indent=2))
    sys.exit(0)
EOF
