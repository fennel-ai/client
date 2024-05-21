---
title: Groupby
order: 0
status: published
---
### Groupby
Operator to group rows of incoming datasets to be processed by the next operator.

Technically, groupby isn't a standalone operator by itself since its output isn't
a valid dataset. Instead, it becomes a valid operator when followed by 
[first](/api-reference/operators/first), [latest](/api-reference/operators/lastest), or [aggregate](/api-reference/operators/aggregate).

#### Parameters

<Expandable title="keys" type="List[str]">
List of keys in the incoming dataset along which the rows should be grouped. This
can be passed as unpacked *args or a Python list.
</Expandable>

<Expandable title="window" type="Optional[Union[Tumbling, Hopping, Session]]">
Optional field to specify the default window for all the aggregations in the following 
aggregate operator. If window parameter is used then the operator can only be followed 
by an aggregate operator and window will become a key field in the output schema.
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
