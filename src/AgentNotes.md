# Agent Notes

## Setting up intstruction

- Create a utility script (called run-tests.sh) in ./utils which can run all unit tests
- Create a utility script (called run-coverage.sh) in ./utils which can run code coverage using all unit tests, run and validate that the script creates output describing the current coverage.
- Create a utility script (called get-project-dependency-order.sh) which returns a simple string array in JSON parsable format representing all the project folders in a particular order.  The output should be ordered to show the projects with the fewest dependees at the start, and the projects with the most dependees at the end.  To determine this order the script should internally determine the project graph hierarchy and then select the leaf nodes first and work its way up the tree to the root node.

## Instructions

- Run get-project-dependency-order.sh
- Determine the code file or class with the least code coverage
