---
title: Log
order: 0
status: published
---
### Log

<Divider>
<LeftSection>
Method to push data into Fennel datasets via [webhook endpoints](/api-reference/sources/webhook).

#### Parameters
<Expandable title="webhook" type="str">
The name of the webhook source containing the endpoint to which the data should 
be logged.
</Expandable>

<Expandable title="endpoint" type="str">
The name of the webhook endpoint to which the data should be logged.
</Expandable>

<Expandable title="df" type="pd.Dataframe">
The dataframe containing all the data that must be logged. The column of the 
dataframe must have the right names & types to be compatible with schemas of
datasets attached to the webhook endpoint.
</Expandable>

<Expandable title="batch_size" type="int" defaultVal="1000">
To prevent sending too much data in one go, Fennel client divides the dataframe
in chunks of `batch_size` rows each and sends each chunk one by one.

Note that Fennel servers provides atomicity guarantee for any call of `log` - either
the whole data is accepted or none of it is. However, breaking down a dataframe
in chunks can lead to situation where some chunks have been ingested but others
weren't.
</Expandable>

#### Errors
<Expandable title="Invalid webhook endpoint">
Fennel will throw an error (equivalent to 404) if no endpoint with the given
specification exists.
</Expandable>

<Expandable title="Schema mismatch errors">
There is no explicit schema tied to a webhook endpoint - the schema comes from 
the datasets attached to it. As a result, the `log` call itself doesn't check for
schema mismatch but later runtime errors may be generated async if the logged 
data doesn't match the schema of the attached datasets.

You may want to keep an eye on the 'Errors' tab of Fennel console after 
initiating any data sync.
</Expandable>
</LeftSection>
<RightSection>

<pre snippet="api-reference/client/log#basic" status="success" 
    message="Logging data to webhook via client"
> </pre>
</RightSection>
</Divider>