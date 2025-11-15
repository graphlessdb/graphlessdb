#!/bin/bash

# Get the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Get the project dependency order
projects=$("$SCRIPT_DIR/get-project-dependency-order.sh")

# Parse the JSON array and iterate over each project
echo "$projects" | jq -r '.[]' | while read -r project; do
    # Run coverage for this project and get the results
    coverage=$("$SCRIPT_DIR/run-project-coverage.sh" "$project" 1)

    # Get the first item's notCovered value
    notCovered=$(echo "$coverage" | jq -r '.[0].lines.notCovered // 0')

    # If notCovered is non-zero, output the name and exit
    if [ "$notCovered" -ne 0 ]; then
        echo "$coverage" | jq -r '.[0].name'
        exit 0
    fi
done
