---
argument-hint: [type]
description: Create an issue on GitHub for unit testing a type
---

Create an issue on GitHub for unit testing a / the type $1.

## Your task

- Determine the type that the user wants to create a GitHub 'unit testing' issue for.  If you don't know the type then ask the user.
- Ensure you can find the type in the codebase before proceeding to create the GitHub issue.
- Use the get-file-coverage.sh script to determine the current code coverage for the type.
- If you have found the type then create a suitable GitHub issue detailing the name and the path to the file relative to the root of the project and summary information about the current code coverage.
- If the code for the type is particularly large and complex you must create a "GitHub sub-issue" using the GitHub project system, do this for each method which lacks full code coverage.  Each "GitHub sub-issue" should be dedicated to a particular well defined method, property, etc within the type such that full coverage of that area will mean that the sub-issue can be marked as complete. GitHub sub-issues should be created for private or directly inaccessible methods, not just the publicly accessible ones.
