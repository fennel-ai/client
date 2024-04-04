---
title: Webhook
order: 0
status: published
---
### Webhook
A push-based data connector, making it convenient for sending arbitrary JSON data 
to Fennel. Data can be pushed to a webhook endpoint either via the REST API or via 
the Python SDK.

#### Source Parameters
<Expandable title="name" type="str">
A name to identify the source. This name should be unique across all Fennel sources.
</Expandable>

<Expandable title="retention" type="Duration" defaultVal="14d">
Data sent to webhook is buffered for the duration `retention`. That is, if the 
data has been logged to a webhook, datasets defined later that source from this 
webhook will still see that data until this duration.
</Expandable>

#### Connector Parameters
<Expandable title="endpoint" type="str">
The endpoint for the given webhook to which the data will be sent.

A single webhook could be visualized as a single Kafka cluster with each endpoint
being somewhat analogous to a topic. A single webhook source can have as many
endpoints as required.

Multiple datasets could be reading from the same webhook endpoint - in which case,
they all get the exact same data.
</Expandable>

<pre snippet="api-reference/sources/webhook#webhook_define"
    status="success" message="Two datasets sourcing from endpoints of the same webook"
    highlight="4, 6, 13">
</pre>
<pre snippet="api-reference/sources/webhook#log_data_sdk"
    status="success" message="Pushing data into webhook via Python SDK">
</pre>
<pre snippet="api-reference/sources/webhook#log_data_rest_api"
    status="success" message="Pushing data into webhook via REST API">
</pre>

#### Errors
<Expandable title="Schema mismatch errors">
Schema validity of data only be checked at runtime. Any rows that 
can not be parsed are rejected. Please keep an eye on the 'Errors' tab of 
Fennel console after initiating any data sync.
</Expandable>

:::info
Unlike all other sources, Webhook does work with mock client. As a result, it's 
very effective for quick prototyping and unit testing.
:::

