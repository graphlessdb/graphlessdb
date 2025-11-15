#!/bin/zsh

# Script to return project folders in dependency order (leaf nodes first)
# Analyzes .csproj files to determine the dependency graph

set -e

# Get the directory where the script is located
SCRIPT_DIR="${0:A:h}"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SRC_DIR="$PROJECT_ROOT/src"

# Find all .csproj files
typeset -A dependencies
typeset -A projects
typeset -A depths
all_projects=()

# Read all project files
while IFS= read -r -d '' csproj_file; do
    project_dir=$(dirname "$csproj_file")
    project_name=$(basename "$project_dir")

    all_projects+=("$project_name")
    projects["$project_name"]="$project_dir"

    # Extract ProjectReference dependencies using grep and sed
    deps=()
    while IFS= read -r dep_name; do
        [[ -n "$dep_name" ]] && deps+=("$dep_name")
    done < <(grep 'ProjectReference' "$csproj_file" 2>/dev/null | grep -o 'Include="\.\.[^"]*' | sed 's|Include="\.\.[/\\]||' | sed 's|[/\\].*||' || true)

    # Store dependencies as comma-separated string
    dependencies["$project_name"]=$(IFS=,; echo "${deps[*]}")
done < <(find "$SRC_DIR" -name "*.csproj" -print0)

# Function to calculate dependency depth (number of dependees)
calculate_depth() {
    local project=$1
    local visited=$2

    # Check for circular dependency
    if [[ $visited == *",$project,"* ]]; then
        echo 0
        return
    fi

    # Access global dependencies array - need quotes around subscript in zsh
    local deps_str="${dependencies["$project"]}"
    if [[ -z "$deps_str" ]]; then
        echo 0
        return
    fi

    local max_depth=0
    # Split on comma using zsh parameter expansion
    local dep_array=("${(@s:,:)deps_str}")
    for dep in "${dep_array[@]}"; do
        if [[ -n "$dep" ]]; then
            local dep_depth=$(calculate_depth "$dep" "$visited,$project,")
            if (( dep_depth >= max_depth )); then
                max_depth=$((dep_depth + 1))
            fi
        fi
    done

    echo $max_depth
}

# Calculate depths for all projects
for project in "${all_projects[@]}"; do
    depth=$(calculate_depth "$project" ",")
    depths[$project]=$depth
done

# Sort projects by depth (ascending)
sorted_projects=()
while IFS= read -r line; do
    project=$(echo "$line" | cut -d' ' -f2)
    sorted_projects+=("$project")
done < <(for project in "${all_projects[@]}"; do
    echo "${depths[$project]} $project"
done | sort -n)

# Output as JSON array
echo -n "["
first=true
for project in "${sorted_projects[@]}"; do
    if [ "$first" = true ]; then
        first=false
    else
        echo -n ","
    fi
    echo -n "\"$project\""
done
echo "]"
