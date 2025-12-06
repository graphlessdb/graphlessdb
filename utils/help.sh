#!/bin/bash

cat <<'EOF'
Utils Scripts:

  begin-issue.sh                        - Find a GitHub issue with status "Todo" and update it to "In Progress", returns issue number
  create-unit-test-issues.sh [limit]    - Create GitHub issues for least covered files (default limit: 10)
  get-crap-scores.sh [--json]           - Calculate CRAP scores for all types/functions, outputs JSON array ordered by worst score first
  get-file-coverage.sh <file-path>      - Get coverage statistics for a specific file
  get-least-covered-files.sh [limit]    - Get least covered files across all non-test projects, outputs JSON (default limit: 10)
  get-project-coverage.sh <path> [lim]  - Run coverage for a single project's tests, outputs JSON array of files with low coverage (default limit: 5)
  get-project-dependency-order.sh       - Get all projects in dependency order (projects with fewer dependents first), outputs JSON array
  get-solution-coverage.sh              - Run coverage for all tests in solution, outputs JSON with line/branch coverage percentages
  run-build.sh                          - Build the GraphlessDB.sln solution with optimal settings
  run-tests.sh                          - Run all tests in GraphlessDB.sln solution (requires prior build)

EOF
