# Implement Issue

This command describes how to implement a GitHub issue.

## General Notes

- You do NOT need to ask me to proceed at any stage.
- Carry out each implementation step in order, ensure that each step is successful before continuing to the next.
- While carrying out the implementation steps you should maintain a temporary file listing unexpected errors arising from incorrect usage of scripts, commands and tools.  This file will be reviewed at the end of the implemenation process to correct our understanding before starting the next issue implementation.

## Git Notes

- You will be working mainly within a git worktree, once the worktree is created, cwd into the worktree, change back out of the worktree directory at the end of the task
- git worktree: Check for existing branches before creating - use ```git worktree add <path> <existing-branch>``` if branch exists, not -b flag

## GitHub Notes

- gh pr create: Must execute from within the worktree directory
- When creating the PR ensure that the branch can be merged back to main without conflicts by pulling latest from remote

## Coding Notes

- Follow existing project naming conventions, use PascalCase without underscores for method names including test methods (e.g., CanGetDateTimePropertyAsString and not Can_Get_DateTime_Property_As_String)
- Helper methods in test classes should be static when possible to follow project conventions
- Manual mock classes are preferred over Moq framework in this project

## Script Notes

- get-file-coverage.sh: Use positional argument, not environment variable - ./utils/get-file-coverage.sh "path/to/file" not FILE_PATH="path"
  ./utils/get-file-coverage.sh
- Coverage tools require full rebuild to get accurate numbers - don't rely on --no-build

## Dotnet Notes

- Ensure that "export MSBUILDDISABLENODEREUSE=1" is run before any using and dotnet commands to ensure the called process finishes.
- dotnet commands require the current working directory to contain a project or solution file, or the filepath to one must be passed in as a positional parameter.  E.g. dotnet clean src/GraphlessDB.sln --nodereuse:false or dotnet build src/GraphlessDB.sln --nodereuse:false

## Implementation Steps

- Ensure you are on the main git branch.
- Ensure you have the pulled the latest from remote.
- Ensure the branch is in a clean state.
- Run ./utils/begin-issue.sh, it should put a GitHub issue into progress and return the issue id.
- Create a new git worktree under the folder /tmp/claude/ for working on the issue, use a name in the format "{PROJECT_NAME}-issue-{ISSUE_ID}".
- Use cwd to change the working directory to the worktree root folder.
- Use the issue id to read the information such as title, description or comments from the issue to determine the file which requires additional unit tests and coverage.
- Determine if the code under test is more suited to unit testing or integration testing.
- Implement any missing unit and / or integration tests for the file under test.
- Iterate using new output from ```./utils/get-file-coverage.sh <file-path>``` until coverage has reached 100%, if 100% is not possible then try to achieve coverage as high as practically possible.
- Once coverage is achieved then create a PR so that the changes can be reviewed.
- Run ```./utils/get-solution-coverage.sh``` and add this information to the PR along with any other important information.
- Clean up the local git worktree.
- Review of any errors that occurred when calling scripts or utility functions and recommend changes to the way you used it for next time.
- Carry out a final review of any exceptional behaviour during the implemenation of this issue, use the temporary file describes earlier to do this.  Determine instances where your understanding of global project concepts / rules has changed due to compilation errors, unit testing errors, script or command invocation errors.  Don't include instances where the learning includes references to particular classes or interfaces, but do include reference to predefined scripts and commands because they are likely reused often.  Generally include items which could be useful to correct for future requests.  If you were to be given your running list back to you again at the beginning of this request then you wouldn't have made as many incorrect assumptions.  Update the PR with this list, it should be extremely concise and be a single short line per item.
