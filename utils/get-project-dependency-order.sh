#!/bin/bash

# Returns JSON array of projects ordered by dependency (leaf nodes first)
# Format: [{name: "", path: "", isTestProject: true}, ...]

set -e

# Navigate to project root
cd "$(dirname "$0")/.."

# Create temporary files to store project info
TEMP_DIR=$(mktemp -d)
PROJ_INFO="$TEMP_DIR/projects.txt"

# Find all csproj files and gather info
find src -name "*.csproj" -type f | while IFS= read -r proj; do
  # Get project name (without path and extension)
  proj_name=$(basename "$proj" .csproj)

  # Check if it's a test project
  if [[ "$proj_name" == *"Tests"* ]] || grep -q "Microsoft.NET.Test.Sdk" "$proj"; then
    is_test="true"
  else
    is_test="false"
  fi

  # Extract project references (dependencies)
  deps=$(grep -o '<ProjectReference Include="[^"]*"' "$proj" 2>/dev/null | grep -o '"[^"]*"' | tr -d '"' | xargs -I {} basename {} .csproj | tr '\n' ',' | sed 's/,$//' || echo "")

  # Store: name|path|isTest|dependencies
  echo "$proj_name|$proj|$is_test|$deps" >> "$PROJ_INFO"
done

# Function to count dependees for a project
count_dependees() {
  local target=$1
  local count=0

  while IFS='|' read -r name path is_test deps; do
    if [[ ",$deps," == *",$target,"* ]]; then
      ((count++))
    fi
  done < "$PROJ_INFO"

  echo $count
}

# Build list with dependee counts
PROJ_WITH_COUNTS="$TEMP_DIR/with_counts.txt"
while IFS='|' read -r name path is_test deps; do
  count=$(count_dependees "$name")
  echo "$count|$name|$path|$is_test" >> "$PROJ_WITH_COUNTS"
done < "$PROJ_INFO"

# Sort by count and build JSON
echo "["
first=true
sort -n "$PROJ_WITH_COUNTS" | while IFS='|' read -r count name path is_test; do
  if [ "$first" = true ]; then
    first=false
  else
    echo ","
  fi

  echo -n "  {\"name\": \"$name\", \"path\": \"$path\", \"isTestProject\": $is_test}"
done
echo ""
echo "]"

# Clean up
rm -rf "$TEMP_DIR"
