# Implement Issue

## Important

- Carry out a final review is issues which occurred during implementation of this request.  Determine instances where your understanding of global project concepts / rules has changed due to compilation errors, unit testing errors, script or command invocation errors.  Don't include instances where the learning includes references to particular classes or interfaces, but do include reference to predefined scripts and commands because they are likely reused often.  Generally include items which could be useful to correct for future requests.  If you were to be given your running list back to you again at the beginning of this request then you wouldn't have made as many incorrect assumptions.  Output this list once the request has been completed successfully, the list should be extremely concise and be a single short line per item.
- Follow existing project naming conventions, use PascalCase without underscores for method names including test methods (e.g., CanGetDateTimePropertyAsString and not Can_Get_DateTime_Property_As_String)
- Helper methods in test classes should be static when possible to follow project conventions
- Coverage tools require full rebuild to get accurate numbers - don't rely on --no-build
- Manual mock classes are preferred over Moq framework in this project
- Before using dotnet clean, dotnet build or other dotnet commands ensure that "export MSBUILDDISABLENODEREUSE=1" is set because dotnet processes continue to run in the background even if they fail.

## Request

Run ./utils/begin-issue.sh which should put a GitHub issue into progress and return the issue id.  Start by creating a new git worktree for working on the issue with a name in the format "issue-{ISSUE_ID}", make sure to set the cwd to the worktree folder.  Next, using the issue id read the information such as title, description or comments from the issue to determine the file which requires additional unit tests and coverage. Next, determine and implement any missing unit tests for that file. You should iterate using new output from ```./utils/get-file-coverage.sh <file-path>``` until the nonCovered lines has reached 0.  Once complete create a PR so that the changes can be reviewed, run ```./utils/get-solution-coverage.sh``` and add the information to the PR, then clean up the local git worktree and finally carry out the final review of any errors that occurred when calling scripts or utility functions and recommend changes to the way you used it for next time.
