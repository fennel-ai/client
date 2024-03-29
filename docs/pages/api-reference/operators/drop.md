---
title: Drop
order: 0
status: published
---
### Drop
Operator to drop one or more non-key non-timestamp columns from a dataset.

#### Parameters

<Expandable title="columns" type="List[str]">
List of columns in the incoming dataset that should be dropped. This can be passed
either as unpacked *args or as a Python list.
</Expandable>

<pre snippet="api-reference/operators/drop#basic" status="success"
    message="Can pass names via *args or kwarg columns" highlight="21, 22">
</pre>

#### Returns

<Expandable type="Dataset">
Returns a dataset with the same schema as the input dataset but with some columns
(as specified by `columns`) removed.
</Expandable>

#### Errors
<Expandable title="Dropping key/timestamp columns">
Commit error on removing any key columns or the timestamp column.
</Expandable>

<Expandable title="Dropping non-existent columns">
Commit error on removing any column that doesn't exist in the input dataset.
</Expandable>

<pre snippet="api-reference/operators/drop#incorrect_type" status="error"
    message="Can not drop key or timestamp columns" highlight="16">
</pre>
<pre snippet="api-reference/operators/drop#missing_column" status="error"
    message="Can not drop a non-existent column" highlight="17">
</pre>
