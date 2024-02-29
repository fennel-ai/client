---
title: Sync
order: 0
status: published
---

### Sync

<Divider>
<LeftSection>
Sends the local dataset and featureset definitions to the server for verification,
storage and processing.

#### Parameters
<Expandable title="datasets" type="List[Dataset]" defaultVal="[]">
List of dataset objects to be synced.
</Expandable>

<Expandable title="featuresets" type="List[Featureset]" defaultVal="[]">
List of featureset objects to be synced.
</Expandable>

<Expandable title="preview" type="bool" defaultVal="False">
If set to True, server only provides a preview of what will happen if sync were
to be done but doesn't change the state at all.
</Expandable>
:::info
Since preview's main goal is to check the validity of old & new definitions, 
it only works with real client/server and mock client/server ignore it.
:::

<Expandable title="tier" type="Optional[str]" defaultVal="None">
Selector to optionally sync only a subset of sources, pipelines and extractors -
those with matching values. Rules of selection:
- If `tier` is None, all objects are selected
- If `tier` is not None, an object is selected if its own selector is either None
  or same as `tier` or is `~x` for some other x
</Expandable>


</LeftSection>
<RightSection>
<pre snippet="api-reference/client/sync#basic" status="success"
    message="Silver source and no extractor are synced" highlight="7-8, 21, 25-29">
</pre>
</RightSection>
</Divider>