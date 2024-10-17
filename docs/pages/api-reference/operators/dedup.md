---
title: Dedup
order: 0
status: published
---
### Dedup
Operator to dedup keyless datasets (e.g. event streams).

#### Parameters
<Expandable title="by" type="Optional[List[str]]" defaultVal="None">

The list of columns to use for identifying duplicates. If not specified, all 
the columns are used for identifying duplicates.

If window is specified, two rows of the input dataset are considered duplicates when they are in the same window and have the same value for the by columns. 

If window is not specified, two rows are considered duplicates when they have the exact same values for the timestamp column and all the by columns.
</Expandable>

<Expandable title="window" type="Optional[Tumbling | Session]" defaultVal="None">

The window to group rows for deduping. If not specified, the rows will be deduped only by the `by` columns and the timestamp.

</Expandable>

<pre snippet="api-reference/operators/dedup#basic" status="success"
    message="Dedup using txid and timestamp">
</pre>

<pre snippet="api-reference/operators/dedup#dedup_by_all" status="success"
    message="Dedup using all the fields">
</pre>

<pre snippet="api-reference/operators/dedup#dedup_with_session_window" status="success"
    message="Dedup using session window">
</pre>

<pre snippet="api-reference/operators/dedup#dedup_with_tumbling_window" status="success"
    message="Dedup using tumbling window">
</pre>

#### Returns
<Expandable type="Dataset">
Returns a keyless dataset having the same schema as the input dataset but with
some duplicated rows filtered out.
</Expandable>

#### Errors
<Expandable title="Dedup on dataset with key columns">
Commit error to apply dedup on a keyed dataset.
</Expandable>


<Expandable title="Dedup on hopping window or tumbling window with lookback">
Dedup on hopping window or tumbling window with lookback is not supported.
</Expandable>
