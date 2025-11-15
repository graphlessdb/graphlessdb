#!/bin/zsh

# This script analyzes .NET project dependencies and returns them in dependency order
# Output: JSON array with format [{name: "", path: "", isTestProject: true}]
# Order: Leaf nodes (fewest dependees) first, root nodes (most dependees) last

set -e

# Get the script directory and repository root
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$REPO_ROOT"

# Use Python to parse projects and calculate dependency order
python3 - <<'PYTHON_SCRIPT'
import os
import re
import json
from pathlib import Path
from collections import defaultdict, deque

# Find all .csproj files
project_files = []
for root, dirs, files in os.walk("src"):
    for file in files:
        if file.endswith(".csproj"):
            project_files.append(os.path.join(root, file))

# Parse project information
projects = {}
dependencies = defaultdict(list)

for proj_path in sorted(project_files):
    with open(proj_path, 'r') as f:
        content = f.read()

    # Get project name
    proj_name = os.path.basename(proj_path).replace('.csproj', '')

    # Check if it's a test project
    is_test = (
        '.Tests' in proj_name or
        'Microsoft.NET.Test.Sdk' in content or
        'Sdk="Microsoft.NET.Test.Sdk"' in content
    )

    projects[proj_name] = {
        'name': proj_name,
        'path': proj_path,
        'isTestProject': is_test
    }

    # Extract ProjectReference dependencies
    proj_refs = re.findall(r'<ProjectReference\s+Include="([^"]+)"', content)
    for ref_path in proj_refs:
        # Get just the project name from the reference (handle both / and \ separators)
        dep_name = ref_path.replace('\\', '/').split('/')[-1].replace('.csproj', '')
        dependencies[proj_name].append(dep_name)

# Topological sort using Kahn's algorithm
# Calculate in-degrees (number of dependencies each project has)
in_degree = {name: 0 for name in projects}
for proj_name, deps in dependencies.items():
    in_degree[proj_name] = len(deps)

# Build reverse dependency graph (dependency -> dependents)
dependents = defaultdict(list)
for proj_name, deps in dependencies.items():
    for dep in deps:
        dependents[dep].append(proj_name)

# Queue of nodes with no dependencies
queue = deque([name for name, degree in in_degree.items() if degree == 0])
result = []

while queue:
    current = queue.popleft()
    result.append(current)

    # Process all dependents
    for dependent in dependents[current]:
        in_degree[dependent] -= 1
        if in_degree[dependent] == 0:
            queue.append(dependent)

# Generate JSON output
output = []
for proj_name in result:
    proj_info = projects[proj_name]
    output.append({
        'name': proj_info['name'],
        'path': proj_info['path'],
        'isTestProject': proj_info['isTestProject']
    })

print(json.dumps(output, indent=2))
PYTHON_SCRIPT
