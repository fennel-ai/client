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
it only works with the real client/server. Mock client/server simply ignores it.
:::
</Expandable>

<Expandable title="incremental" type="bool" defaultVal="False">
If set to True, Fennel assumes that only those datasets/featuresets are
provided to `commit` operation that are potentially changing in any form. Any 
previously existing datasets/featuresets that are not included in the `commit` 
operation are left unchanged.
</Expandable>

<Expandable title="env" type="Optional[str]" defaultVal="None">
Selector to optionally commit only a subset of sources, pipelines and extractors -
those with matching values. Rules of selection:
- If `env` is None, all objects are selected
- If `env` is not None, an object is selected if its own selector is either None
  or same as `env` or is `~x` for some other x
</Expandable>

<Expandable title="backfill" type="bool" defaultVal="True">
If you set the backfill parameter to False, the system will return an error if committing changes would result in a backfill of any dataset/pipeline.
A backfill occurs when there is no existing dataset that is isomorphic to the new dataset.
Setting backfill to False helps prevent accidental backfill by ensuring that only datasets matching the existing structure are committed.
</Expandable>

<pre snippet="api-reference/client/commit#basic" status="success"
    message="Silver source and no extractor are committed">
</pre>

<pre snippet="api-reference/client/commit#incremental" status="success"
    message="Second commit adds a featureset & leaves dataset unchanged">
</pre>

<pre snippet="api-reference/client/commit#backfill" status="success"
    message="Backfill param will prevent backfill of Transaction dataset when committing to main branch">
</pre>
