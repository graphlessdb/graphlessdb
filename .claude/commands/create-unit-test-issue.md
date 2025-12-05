---
argument-hint: [type]
description: Create an issue on GitHub for unit testing a type
---

Create an issue on GitHub for unit testing a / the type $1.

## Context

- Current gh repo !`gh repo view --json nameWithOwner`

## Script notes

- Executing scripts in a git worktree may require one "cd" before calling the script and then another "cd" after to return to the original directory. E.g. cd /private/tmp/claude/graphlessdb-issue-164 && git pull origin main && cd /users/blah/github/graphlessdb
- get-file-coverage.sh: Use positional argument, not environment variable - ./utils/get-file-coverage.sh "path/to/file" not FILE_PATH="path"
  ./utils/get-file-coverage.sh
- Coverage tools require full rebuild to get accurate numbers - don't rely on --no-build

## Your task

- Determine the type that the user wants to create a GitHub 'unit testing' issue for.  If you don't know the type then ask the user.
- Ensure you can find the type in the codebase before proceeding to create the GitHub issue.
- Use the get-file-coverage.sh script to determine the current code coverage for the type.
- If you have found the type then create a suitable GitHub issue detailing the name and the path to the file relative to the root of the project and summary information about the current code coverage.
- If the code for the type is particularly large and complex, with areas that may be hard to test then you should create additional sub-tasks for each method which lacks full code coverage.  Sub-tasks for methods should be created in order of methods deepest in the call stack first.  Sub-tasks can and should be created for private or directly inaccessible methods.
