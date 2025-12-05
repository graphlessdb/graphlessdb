---
argument-hint: [type]
description: Create an issue on GitHub for unit testing a type
---

Create an issue on GitHub for unit testing a / the type $1.

## Context

- Current GitHub project, including project number !`gh project list --owner graphlessdb`
- Current GitHub repository and owner !`gh repo view --json nameWithOwner`
- GitHub cli project help !`gh project --help`
- GitHub cli project field-list help !`gh project field-list --help`
- GitHub cli project item-edit help !`gh project item-edit --help`

## GitHub notes

- use gh (GitHub cli) to work with github rather than using the MCP.
- You MUST check the current repository name using GitHub cli before getting or adding GitHub issues.
- gh pr create: Must execute from within the worktree directory.
- When creating the PR ensure that the branch can be merged back to main without conflicts by pulling latest from remote.

## Process to create a GitHub sub-issue

- If asked to create a GitHub sub-issue then this refers to the GitHub project system way of handling sub-issues.
- First, create the sub-issue as you would a regular issue.
- Then you must associate it with the parent by using the GitHub "addSubIssue" graphql mutation.

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
- If the code for the type is particularly large and complex you must create a "GitHub sub-issue" using the GitHub project system, do this for each method which lacks full code coverage.  Each "GitHub sub-issue" should be dedicated to a particular well defined method, property, etc within the type such that full coverage of that area will mean that the sub-issue can be marked as complete. GitHub sub-issues should be created for private or directly inaccessible methods, not just the publicly accessible ones.
