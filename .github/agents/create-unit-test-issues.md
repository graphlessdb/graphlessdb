---
name: create-unit-test-issues
description: Agent specializing in creating GitHub issues for unit test coverage
---

## Implementation Steps

- Ensure that you are on the main branch and have latest source checked.
- Run ./utils/get-least-covered-files.sh 1
- Then create a GitHub issue for each file in the response.
