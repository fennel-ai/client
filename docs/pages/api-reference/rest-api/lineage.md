---
title: Lineage
order: 0
status: published
---

`GET /api/v1/lineage`
### Lineage
Method to get [lineage](/observability/lineage)
via REST API.


#### Headers
<Expandable title="Content-Type" type='"application/json"'>
All Fennel REST APIs expect a content-type of `application/json`.
</Expandable>

<Expandable title="Authorization" type="Bearer {str}">
Fennel uses bearer token for authorization. Pass along a valid token that has
permissions to log data to the webhook.
</Expandable>

<Expandable title="X-FENNEL-BRANCH" type="Bearer {str}">
Fennel uses header for passing branch name to the server against which we want to query.
</Expandable>

<pre snippet="api-reference/rest-api#lineage"></pre>