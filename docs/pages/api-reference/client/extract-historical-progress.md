---
title: Extract Historical Progress
order: 0
status: published
---
### Extract Historical Progress

<Divider>
<LeftSection>
Method to monitor the progress of a run of `extract_historical` query.

#### Parameters
<Expandable title="request_id" type="str">
The unique request ID returned by the `extract_historical` operation that needs
to be tracked.
</Expandable>

#### Returns
<Expandable title="type" type="Dict[str, Any]">
Immediately returns a dictionary containing the following information:
* request_id - a random uuid assigned to this request. Fennel can be polled
  about the status of this request using the `request_id`
* output s3 bucket - the s3 bucket where results will be written
* output s3 path prefix - the prefix of the output s3 bucket
* completion rate - progress of the request as a fraction between 0 and 1
* failure rate - fraction of the input rows (between 0-1) where an error was 
  encountered and output features couldn't be computed
* status - the overall status of this request

A completion rate of 1.0 indicates that all processing has been completed.
A completion rate of 1.0 and failure rate of 0 means that all processing has 
been completed successfully.
</Expandable>
</LeftSection>
<RightSection>
===
<pre name="Request" snippet="api-reference/client/query#extract_historical_progress"
  status="success" message="Checking progress of a prior extract historical request"
></pre>

<pre name="Response" snippet="api-reference/client/query#extract_historical_response"
  status="success" message="Sample response of extract historical progress"
></pre>
===
</RightSection>
</Divider>



