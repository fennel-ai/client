---
title: HTTP
order: 0
status: published
---
### HTTP
Data sink to HTTP endpoints.

#### Connector Parameters
<Expandable title="name" type="str">
A name to identify the sink. The name should be unique across all Fennel connectors.
</Expandable>

<Expandable title="host" type="str|Secret">
The HTTP host URL. Example: https://127.0.0.1:8081
</Expandable>

<Expandable title="healthz" type="str">
The health check endpoint to verify the server's availability.
</Expandable>

#### HTTP Path Parameters
<Expandable title="endpoint" type="str">
The specific endpoint where data will be sent
</Expandable>

<Expandable title="limit" type="Optional[int]">
The number of records to include in each request to the endpoint. Default: 100
</Expandable>

<Expandable title="headers" type="Dict[str,str]">
A map of headers to include with each request
</Expandable>


<pre snippet="api-reference/sinks/http_sink#basic"
    status="success" message="HTTP sink">
</pre>

#### Errors
<Expandable title="Connectivity Issues">
Fennel tries to test the connection with your HTTP sink during `commit` itself
using the health check endpoint

Note: Mock client can not talk to any external data sink and hence is unable to
do this validation at commit time.
</Expandable>

:::info
- HTTP sink ensures at least once delivery. To handle duplicates, use
`["payload"]["source"]["fennel"]["partition"]` and `["payload"]["source"]["fennel"]["offset"]` 
fields in the output.
:::





