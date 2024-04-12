---
title: Aggregate
order: 0
status: published
---
### Aggregate
Operator to do continuous window aggregations. Aggregate operator must always
be preceded by a [groupby](/api-reference/operators/groupby) operator.

#### Parameters

<Expandable title="aggregates" type="List[Aggregation]">
Positional argument specifying the list of aggregations to apply on the grouped
dataset. This list can be passed either as an unpacked *args or as an explicit 
list as the first position argument.

See [aggregations](/api-reference/aggregations) for the full list of aggregate 
functions.
</Expandable>

<pre snippet="api-reference/operators/aggregate#basic" status="success"
    message="Aggregate count & sum of transactions in rolling windows"
>
</pre>

#### Returns
<Expandable type="Dataset">
Returns a dataset where all columns passed to `groupby` become the key columns,
the timestamp column stays as it is and one column is created for each aggregation.

The type of each aggregated column depends on the aggregate and the type of the
corresponding column in the input dataset.
</Expandable>

:::info
Aggregate is the terminal operator - no other operator can follow it and no 
other datasets can be derived from the dataset containing this pipeline.
:::


