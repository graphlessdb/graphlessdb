# Create unit test issues

## Implementation Steps

- Ensure that you are on the main branch and have latest source checked.
- Run ./utils/get-least-covered-files.sh 50 and then filter out any files which have been worked on recently, determine this by fetching the 20 most recent issues from github and comparing the title of the issue with the file name covered file.
- Truncate the list of files down to 10 and then create GitHub issue to implement tests for each one.
