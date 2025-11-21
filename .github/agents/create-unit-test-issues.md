---
name: create-unit-test-issues
description: Agent specializing in creating GitHub issues for unit test coverage
---

## Implementation Steps

- Ensure that you are on the main branch and have latest source checked.
- Run ./utils/get-least-covered-files.sh 50 and then filter out any files which have been worked on recently, determine this by fetching the 20 most recent issues from github and do a fuzzy comparison of the title of the issue with the file name covered file.
- Truncate the list of files down to 10 and then create GitHub issue to implement tests for each one.
