---
argument-hint: [issue-number]
description: Implement a GitHub issue
---

Implement a GitHub issue $1.

## Your task

- If you are a Copilot agent then ensure that you have executed each command under the "Context" section, they provide important context to the task. Each command is specified within an exclamation mark and quotes as follows !``. Those symbols are not part of the command to be executed. If you are a Claude agent then you have likely done this already.
- Ensure you are on the main git branch.
- Ensure you have the pulled the latest from remote.
- Ensure the branch is in a clean state.
- Ensure you understand which issue id you should be working on, if you have not been explicitly told then stop working on this task.
- Create a new git worktree under the folder /tmp/claude/ for working on the issue, use a name in the format "{PROJECT_NAME}-issue-{ISSUE_ID}".
- Use the issue id to read the information such as title, description or comments from the issue to determine the file which requires additional unit tests and coverage.
- Determine if the code under test is more suited to unit testing or integration testing.
- Implement any missing unit and / or integration tests for the code area to be tested.
- Iterate using new output from `./utils/get-file-coverage.sh <file-path>` until coverage for the area to be tested has reached 100%.
- The task cannot be completed if there are failing tests.
- If there were any unresolved elements of the task then update the GitHub issue with a comment detailing the challenges encountered and set out a short set of clearly defined proposed tasks to break down and tackle the task further.
- If coverage was already at 100% then close the issue without raising a PR.
- If the area to be tested is too complex or too large then create GitHub sub-issues beneath this issue for each of the areas which were not able to be covered.
- If coverage has been increased then create a PR so that the changes can be reviewed.
- Clean up the local git worktree.
- Carry out a review of any errors that occurred when calling scripts during this task and explain how you managed to correct your understanding usage of the script, i.e. recommend changes to the way you used it for next time.
