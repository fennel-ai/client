---
title: Groupby
order: 0
status: published
---
### Groupby
Operator to group rows of incoming datasets to be processed by the next operator.

Technically, groupby isn't a standalone operator by itself since its output isn't
a valid dataset. Instead, it becomes a valid operator when followed by 
[first](/api-reference/operators/first), [aggregate](/api-reference/operators/aggregate), 
or [window](/api-reference/operators/window) operators.

#### Parameters

<Expandable title="keys" type="List[str]">
List of keys in the incoming dataset along which the rows should be grouped. This
can be passed as unpacked *args or a Python list.
</Expandable>

<pre snippet="api-reference/operators/groupby#basic" status="success"
    message="Groupby category before using first">
</pre>

#### Errors
<Expandable title="Grouping by non-existent columns">
Commit error if trying to group by columns that don't exist in the input dataset.
</Expandable>

<Expandable title="Grouping by timestamp column">
Commit error if trying to do a groupby via the timestamp column of the input dataset.
</Expandable>

<pre snippet="api-reference/operators/groupby#non_existent_column" status="error"
    message="Groupby using a non-existent column">
</pre>
