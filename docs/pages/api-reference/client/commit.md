---
title: Commit
order: 0
status: published
---

### Commit


Sends the local dataset and featureset definitions to the server for verification,
storage and processing.

#### Parameters
<Expandable title="message" type="str">
Human readable description of the changes in the commit - akin to the commit 
message in a git commit.
</Expandable>

<Expandable title="datasets" type="List[Dataset]" defaultVal="[]">
List of dataset objects to be committed.
</Expandable>

<Expandable title="featuresets" type="List[Featureset]" defaultVal="[]">
List of featureset objects to be committed.
</Expandable>

<Expandable title="preview" type="bool" defaultVal="False">
If set to True, server only provides a preview of what will happen if commit were
to be done but doesn't change the state at all.

:::info
Since preview's main goal is to check the validity of old & new definitions, 
it only works with real client/server and mock client/server ignore it.
:::
</Expandable>

<Expandable title="tier" type="Optional[str]" defaultVal="None">
Selector to optionally commit only a subset of sources, pipelines and extractors -
those with matching values. Rules of selection:
- If `tier` is None, all objects are selected
- If `tier` is not None, an object is selected if its own selector is either None
  or same as `tier` or is `~x` for some other x
</Expandable>

<pre snippet="api-reference/client/commit#basic" status="success"
    message="Silver source and no extractor are committed">
</pre>
