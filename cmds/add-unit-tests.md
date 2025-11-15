# add-unit-tests

## Constant Context

- Whilst carrying out this request, keep a running list of instances where your understanding of global project concepts / rules has changed due to compilation issues or unit testing complications. Don't include instances that are specific to this actual task, instead only include items which are global in nature and will be useful in many future requests.  If you were to be given your running list back to you again at the beginning of this request then you wouldn't have made as many incorrect assumptions.  Output this list once the request has been completed successfully, the list should be extremely concise and be a single short line per item.

## Request

Run get-least-covered-file.sh to get the name of a file which requires unit testing to increase its code coverage.  Then determine and implement any missing unit tests for that file. You should iterate using get-file-coverage.sh until the  nonCovered lines has reached 0.
