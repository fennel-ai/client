---
title: Cancel Offline Query
order: 0
status: published
---
`cancel_offline_query`
### Cancel Offline Query

<Divider>
<LeftSection>
Method to cancel a previously issued `query_offline` request.

#### Parameters
<Expandable title="request_id" type="str">
The unique request ID returned by the `query_offline` operation that needs
to be canceled.
</Expandable>

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
</LeftSection>
<RightSection>
===
<pre name="Request" snippet="api-reference/client/query#extract_historical_cancel"
  status="success" message="Canceling offline query with given ID"
></pre>

<pre name="Response" snippet="api-reference/client/query#extract_historical_response"
  status="success" message="Sample response of cancel_offline_query"
></pre>
===
</RightSection>
</Divider>
