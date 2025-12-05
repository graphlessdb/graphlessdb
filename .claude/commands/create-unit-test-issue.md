---
argument-hint: [type]
description: Create an issue on GitHub for unit testing a type
---

Create an issue on GitHub for unit testing a / the type $1.

## Context

- Current gh repo !`gh repo view --json nameWithOwner`

## Your task

- Determine the type that the user wants to create a GitHub 'unit testing' issue for.  If you don't know the type then ask the user.
- Ensure you can find the type in the codebase before proceeding to create the GitHub issue.
- If you have found the type then create a suitable GitHub issue detailing the name and the path to the file relative to the root of the project.
