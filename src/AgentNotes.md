# Agent Notes

## Setting up intstruction

- Create a utility script (called run-tests.sh) in ./utils which can run all unit tests
- Create a utility script (called run-coverage.sh) in ./utils which can run code coverage using all unit tests, run and validate that the script creates output describing the current coverage.
- Create a utility script (called get-project-dependency-order.sh) which returns a simple string array in JSON parsable format representing all the project folders in a particular order.  The output should be ordered to show the projects with the fewest dependees at the start, and the projects with the most dependees at the end.  To determine this order the script should internally determine the project graph hierarchy and then select the leaf nodes first and work its way up the tree to the root node.
- Create a utility script (called run-project-coverage.sh) which runs code coverage for all the unit tests within that just that project, it should return a simple JSON parsable array.  The array item should be in the format { name: "MyName", lines: {covered: 123, notCovered: 123 } }.  The array should be ordered so that items with larger notCovered appear at the start.  The script should accept an optional limit parameter which reduces the final array output length to that size, the default limit value should be 5.
- Create a utility script (called get-least-covered-file.sh) which runs the get-project-dependency-order.sh script, iterates over the returned project list items and runs the run-project-coverage.sh script on each item one at a time.  After getting the response from run-project-coverage.sh it should check the "notCovered" value of the first item in the array. If the value is non zero it should return the "name" value and exit the script.  If the "notCovered" value is zero then it should carry on iterating through the projects.

## Instructions

- Run get-project-dependency-order.sh
- Iterate Run run-project-coverage.sh for the first project
