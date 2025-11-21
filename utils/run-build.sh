#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

export MSBUILDDISABLENODEREUSE=1

dotnet build "$REPO_ROOT/src/GraphlessDB.sln" \
    --no-incremental \
    -p:UseSharedCompilation=false \
    -p:UseRazorBuildServer=false \
    /nodeReuse:false \
    --verbosity quiet
