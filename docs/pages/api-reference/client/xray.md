---
title: Xray
order: 0
status: published
---

`xray`
### Xray

Get details(metric and metadata) for datasets and featuresets

#### Parameters
<Expandable title="datasets" type="Optional[Union[str, List[str]]]">
List of names of datasets for which details are needed
</Expandable>

<Expandable title="featuresets" type="Optional[Union[str, List[str]]]">
List of names of featuresets for which details are needed
</Expandable>

<Expandable title="properties" type="Optional[Union[str, List[str]]] ">
List of properties (if you need to filter specific properties)
</Expandable>

#### Returns
<Expandable type="Union[List[Dataset],List[Featureset]]">
Returns a detailed list for datasets and featuresets.
</Expandable>


<pre snippet="api-reference/client/xray#basic" status="success"
    message="A valid Xray example">
</pre>
