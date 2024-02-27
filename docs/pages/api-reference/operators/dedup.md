---
title: Dedup
order: 0
status: published
---
### Dedup

<Divider>
<LeftSection>
Operator to dedup keyless datasets (e.g. event streams).

#### Parameters
<Expandable title="by" type="Optional[List[str]]" defaultVal="None">

The list of columns to use for identifying duplicates. If not specified, all 
the columns are used for identifying duplicates.

Two rows of the input dataset are considered duplicates if and only if they have 
the same values for the timestamp column and all the `by` columns.
</Expandable>

#### Returns
<Expandable type="Dataset">
Returns a keyless dataset having the same schema as the input dataset but with
some duplicated rows filtered out.
</Expandable>

#### Errors
<Expandable title="Dedup on dataset with key columns">
Commit error to apply dedup on a keyed dataset.
</Expandable>

</LeftSection>

<RightSection>
<pre snippet="api-reference/operators/dedup#basic" status="success"
    message="Dedup using txid and timestamp">
</pre>

<pre snippet="api-reference/operators/dedup#dedup_by_all" status="success"
    message="Dedup using all the fields">
</pre>
</RightSection>

</Divider>

