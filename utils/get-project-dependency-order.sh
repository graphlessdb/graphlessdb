#!/bin/zsh
set -euo pipefail

# Disable MSBuild node reuse to prevent hanging processes
export MSBUILDDISABLENODEREUSE=1

# Get the script directory (zsh uses $0 instead of BASH_SOURCE)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Find the solution file
SOLUTION_FILE="$REPO_ROOT/src/GraphlessDB.sln"

if [ ! -f "$SOLUTION_FILE" ]; then
    echo "Error: Solution file not found at $SOLUTION_FILE" >&2
    exit 1
fi

# Get all projects in the solution
PROJECTS=$(dotnet sln "$SOLUTION_FILE" list 2>&1 | tail -n +3)

if [ -z "$PROJECTS" ]; then
    echo "Error: No projects found in solution" >&2
    exit 1
fi

# Build a temporary file to store project information
TEMP_FILE=$(mktemp)
trap "rm -f $TEMP_FILE" EXIT

# Parse each project to extract dependencies
typeset -A PROJECT_DEPS
typeset -A PROJECT_PATHS
typeset -A IS_TEST_PROJECT

while IFS= read -r project_path; do
    [ -z "$project_path" ] && continue

    # Full path to project file
    FULL_PATH="$REPO_ROOT/src/$project_path"

    if [ ! -f "$FULL_PATH" ]; then
        echo "Error: Project file not found at $FULL_PATH" >&2
        exit 1
    fi

    # Extract project name (without .csproj)
    PROJECT_NAME=$(basename "$project_path" .csproj)

    # Store the path
    PROJECT_PATHS["$PROJECT_NAME"]="$project_path"

    # Check if it's a test project
    if [[ "$PROJECT_NAME" == *.Tests ]]; then
        IS_TEST_PROJECT["$PROJECT_NAME"]=true
    else
        IS_TEST_PROJECT["$PROJECT_NAME"]=false
    fi

    # Extract ProjectReference dependencies using grep and sed
    # Handle both forward slashes and backslashes in paths
    DEPS=$(grep -o '<ProjectReference Include="[^"]*"' "$FULL_PATH" 2>/dev/null | sed 's/.*Include="\.\.[\\/]\([^\\\/]*\)[\\/].*/\1/' || true)

    # Store dependencies
    PROJECT_DEPS["$PROJECT_NAME"]="$DEPS"
done <<< "$PROJECTS"

# Calculate the dependency depth (number of projects that depend on this one)
# We'll use a topological sort approach - projects with no dependents come first
typeset -A dependency_count
typeset -A processed
ordered_projects=()

# Initialize dependency count for each project
for project in ${(k)PROJECT_DEPS}; do
    dependency_count[$project]=0
done

# Count how many projects depend on each project
for project in ${(k)PROJECT_DEPS}; do
    deps_value="${PROJECT_DEPS[$project]:-}"
    if [ -n "$deps_value" ]; then
        for dep in ${=deps_value}; do
            if [ -n "$dep" ]; then
                # Find the matching key in dependency_count (which may have quotes)
                for dep_key in ${(k)dependency_count}; do
                    dep_key_clean=${dep_key//\"/}
                    if [ "$dep_key_clean" = "$dep" ]; then
                        ((dependency_count[$dep_key]++)) || true
                        break
                    fi
                done
            fi
        done
    fi
done

# Perform topological sort - repeatedly find projects with no dependents
while [ ${#processed[@]} -lt ${#PROJECT_DEPS[@]} ]; do
    found_project=false

    # Find projects with minimum dependency count that haven't been processed
    min_count=999999
    for project in ${(k)PROJECT_DEPS}; do
        if [ -z "${processed[$project]:-}" ]; then
            if [ ${dependency_count[$project]} -lt $min_count ]; then
                min_count=${dependency_count[$project]}
            fi
        fi
    done

    # Process all projects with the minimum count at this level
    for project in ${(k)PROJECT_DEPS}; do
        if [ -z "${processed[$project]:-}" ] && [ ${dependency_count[$project]} -eq $min_count ]; then
            ordered_projects+=("$project")
            processed[$project]=1
            found_project=true

            # Reduce the dependency count for all projects that depend on this one
            for other_project in ${(k)PROJECT_DEPS}; do
                other_deps_value="${PROJECT_DEPS[$other_project]:-}"
                if [ -n "$other_deps_value" ]; then
                    for dep in ${=other_deps_value}; do
                        # Need to compare without quotes
                        project_clean=${project//\"/}
                        if [ "$dep" = "$project_clean" ]; then
                            ((dependency_count[$other_project]--)) || true
                        fi
                    done
                fi
            done
        fi
    done

    if [ "$found_project" = false ]; then
        echo "Error: Circular dependency detected" >&2
        exit 1
    fi
done

# Output as JSON array
echo -n "["
first=true
for project in "${ordered_projects[@]}"; do
    if [ "$first" = true ]; then
        first=false
    else
        echo -n ","
    fi
    # Strip quotes from project name for JSON output
    project_name=${project//\"/}
    echo -n "{\"name\":\"$project_name\",\"path\":\"${PROJECT_PATHS[$project]}\",\"isTestProject\":${IS_TEST_PROJECT[$project]}}"
done
echo "]"
