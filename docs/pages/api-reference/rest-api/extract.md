---
title: Query
order: 1
status: 'published'
---

<Divider>
<LeftSection>
`POST /api/v1/branch/:branch_name/query`
### Query

API to extract a set of output features given known values of some input features. 

#### Headers
<Expandable title="Content-Type" type='"application/json"'>
All Fennel REST APIs expect a content-type of `application/json`.
</Expandable>

<Expandable title="Authorization" type="Bearer {str}">
Fennel uses bearer token for authorization. Pass along a valid token that has
permissions to log data to the webhook.
</Expandable>

#### Query Parameters
<Expandable title="branch_name" type="str">
The name of the branch against which we want to query.
</Expandable>

#### Body Parameters:
<Expandable title="inputs" type="str">
List of fully qualified names of input features. Example name: `Featureset.feature`
</Expandable>

<Expandable title="outputs" type="str">
List of fully qualified names of output features. Example name: `Featureset.feature`. 
Can also contain name of a featureset in which case all features in the featureset
are returned.
</Expandable>

<Expandable title="data" type="json">
JSON representing the dataframe of input feature values. The json can either be 
an array of json objects, each representing a row; or it can be a single json 
object where each key maps to a list of values representing a column. 

Strings of json are also accepted.
</Expandable>

<Expandable title="log" type="bool">
If true, the extracted features are also logged (often to serve as future training data).
</Expandable>

<Expandable title="workflow" type="string" defaultVal="default">
The name of the workflow with which features should be logged (only relevant 
when `log` is set to true).
</Expandable>

<Expandable title="sampling_rate" type="float">
Float between 0-1 describing the sample rate to be used for logging features
(only relevant when `log` is set to `true`).
</Expandable>

#### Returns
The response dataframe is returned as column oriented json.

</LeftSection>
<RightSection>
<pre snippet="api-reference/rest-api#rest_extract_api_columnar"
    status="success" message="With column oriented data">
</pre>

<pre snippet="api-reference/rest-api#rest_extract_api" status="success"
    message="With row oriented data">
</pre>

</RightSection>
</Divider>

