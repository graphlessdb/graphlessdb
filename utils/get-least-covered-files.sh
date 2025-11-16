#!/bin/zsh

# Get the least covered files across all projects
# Usage: ./get-least-covered-files.sh [limit]
# Output: JSON array of files ordered by notCovered lines (descending)

set -e

# Get the script directory and repository root
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Default limit
LIMIT="${1:-5}"

# Get project dependency order and pipe to Python script
"$SCRIPT_DIR/get-project-dependency-order.sh" | python3 -c '
import sys
import json
import subprocess
import re

def main():
    if len(sys.argv) < 3:
        print("Error: Missing arguments", file=sys.stderr)
        sys.exit(1)

    projects_json = sys.stdin.read()
    script_dir = sys.argv[1]
    limit = int(sys.argv[2])

    projects = json.loads(projects_json)

    # Patterns to identify compiler-generated classes
    COMPILER_GENERATED_PATTERNS = [
        r"^<.*>",  # Starts with angle bracket (e.g., <>c__DisplayClass)
        r"__DisplayClass",  # Display classes
        r"__StaticArrayInit",  # Static array initializers
        r"\+<>c$",  # Nested compiler-generated closure classes
        r"\+<.*>d__\d+$",  # Async state machines
        r"/<.*>d__\d+$",  # Iterator state machines
        r"^Program\$<.*>$",  # Top-level statement classes
        r"\$<.*>$",  # Other compiler-generated classes
    ]

    # Patterns to identify compiler-generated file paths
    COMPILER_GENERATED_PATH_PATTERNS = [
        r"/obj/",  # Files in obj directory (build artifacts)
        r"/bin/",  # Files in bin directory (build artifacts)
        r"\.g\.cs$",  # Generated source files (.g.cs extension)
        r"\.Designer\.cs$",  # Designer-generated files
        r"\.AssemblyInfo\.cs$",  # Assembly info files
        r"\.AssemblyAttributes\.cs$",  # Assembly attribute files
    ]

    def is_compiler_generated(class_name, file_path=""):
        """Check if a class name or file path matches compiler-generated patterns"""
        # Check class name patterns
        for pattern in COMPILER_GENERATED_PATTERNS:
            if re.search(pattern, class_name):
                return True

        # Check file path patterns
        for pattern in COMPILER_GENERATED_PATH_PATTERNS:
            if re.search(pattern, file_path):
                return True

        return False

    def should_include(item):
        """Determine if an item should be included in results"""
        not_covered = item["lines"]["notCovered"]
        covered = item["lines"]["covered"]

        # Filter out items with notCovered = 0
        if not_covered == 0:
            return False

        # Filter out compiler-generated classes
        if is_compiler_generated(item["name"], item.get("path", "")):
            return False

        # Calculate coverage ratio
        total_lines = covered + not_covered
        if total_lines == 0:
            return False

        coverage_ratio = covered / total_lines

        # Filter out items with >= 80% coverage
        if coverage_ratio >= 0.8:
            return False

        return True

    # Collect results from all non-test projects
    all_results = []

    for project in projects:
        # Skip test projects
        if project["isTestProject"]:
            continue

        # Find corresponding test project
        project_name = project["name"]
        test_project_name = f"{project_name}.Tests"

        # Look for the test project in the projects list
        test_project = None
        for p in projects:
            if p["name"] == test_project_name and p["isTestProject"]:
                test_project = p
                break

        # Skip if no test project found
        if not test_project:
            continue

        # Run coverage for the test project
        try:
            result = subprocess.run(
                [f"{script_dir}/run-project-coverage.sh", test_project["path"], "1000"],
                capture_output=True,
                text=True,
                check=True
            )

            coverage_data = json.loads(result.stdout)

            # Filter and append results
            for item in coverage_data:
                if should_include(item):
                    all_results.append(item)

                    # Check if we have reached the limit
                    if len(all_results) >= limit:
                        break

            # Check if we have reached the limit
            if len(all_results) >= limit:
                break

        except subprocess.CalledProcessError:
            # Skip projects that fail to run coverage
            continue
        except json.JSONDecodeError:
            # Skip projects with invalid JSON output
            continue

    # Sort by notCovered (descending), then by name for consistency
    all_results.sort(key=lambda x: (-x["lines"]["notCovered"], x["name"]))

    # Truncate to limit
    all_results = all_results[:limit]

    # Output JSON
    print(json.dumps(all_results, indent=2))

main()
' "$SCRIPT_DIR" "$LIMIT"
