---
title: Log
order: 0
status: published
---

`POST /api/v1/log`
### Log
Method to push data into Fennel datasets via [webhook endpoints](/api-reference/source_connectors/webhook)
via REST API.


#### Headers
<Expandable title="Content-Type" type='"application/json"'>
All Fennel REST APIs expect a content-type of `application/json`.
</Expandable>

<Expandable title="Authorization" type="Bearer {str}">
Fennel uses bearer token for authorization. Pass along a valid token that has
permissions to log data to the webhook.
</Expandable>


#### Body Parameters
<Expandable title="webhook" type="str">
The name of the webhook source containing the endpoint to which the data should 
be logged.
</Expandable>

<Expandable title="endpoint" type="str">
The name of the webhook endpoint to which the data should be logged.
</Expandable>

<Expandable title="data" type="json">
The data to be logged to the webhook. This json string could either be:
- Row major where it's a json array of rows with each row written as a json object.

- Column major where it's a dictionary from column name to values of that 
  column as a json array.
</Expandable>

<pre snippet="api-reference/rest-api#rest_log_api"></pre>