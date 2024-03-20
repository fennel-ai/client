---
title: Extract Historical Cancel
order: 0
status: published
---
### Extract Historical Cancel Request
Method to cancel a previously issued `extract_historical` request.

#### Parameters
<Expandable title="request_id" type="str">
The unique request ID returned by the `extract_historical` operation that needs
to be canceled.
</Expandable>

===
<pre name="Request" snippet="api-reference/client/extract#extract_historical_cancel"
  status="success" message="Canceling extract historical request with given ID"
></pre>

<pre name="Response" snippet="api-reference/client/extract#extract_historical_response"
  status="success" message="Sample response of extract historical cancellation"
></pre>
===

#### Returns
<Expandable title="type" type="Dict[str, Any]">
Marks the request for cancellation and immediately returns a dictionary 
containing the following information:
* request_id - a random uuid assigned to this request. Fennel can be polled
  about the status of this request using the `request_id`
* output s3 bucket - the s3 bucket where results will be written
* output s3 path prefix - the prefix of the output s3 bucket
* completion rate - progress of the request as a fraction between 0 and 1
* failure rate - fraction of the input rows (between 0-1) where an error was 
  encountered and output features couldn't be computed
* status - the overall status of this request

A completion rate of 1.0 indicates that all processing had been completed.
A completion rate of 1.0 and failure rate of 0 means that all processing had 
been completed successfully.
</Expandable>
