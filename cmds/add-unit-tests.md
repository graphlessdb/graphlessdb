# add-unit-tests

## Constant Context

Whilst carrying out this request, keep a running list of instances where your understanding of global project concepts / rules has changed due to compilation errors or unit testing errors. Don't include instances that are specific to this actual task.  Don't include instances where the learning includes references to particular classes or interfaces. Instead only include items which are global in nature and will be useful in many future requests.  If you were to be given your running list back to you again at the beginning of this request then you wouldn't have made as many incorrect assumptions.  Output this list once the request has been completed successfully, the list should be extremely concise and be a single short line per item.

## Learnt Context

- Test method naming convention: PascalCase without underscores (e.g., CanGetDateTimePropertyAsString not Can_Get_DateTime_Property_As_String)
- Helper methods in test classes should be static when possible to follow project conventions
- Coverage tools require full rebuild to get accurate numbers - don't rely on --no-build

## Request

Run get-least-covered-file.sh and read its output to get the name of a file which requires unit testing to increase its code coverage.  Then determine and implement any missing unit tests for that file. You should iterate using new output from get-file-coverage.sh until the nonCovered lines has reached 0.  Once complete run save-solution-coverage.sh.
