---
title: Aggregate
order: 0
status: published
---
# Aggregate

<Divider>
<LeftSection>
Operator to do continuous window aggregations. Aggregate operator must always
be preceded by a [groupby](/api-reference/operators/groupby) operator. The 
aggregation logic is applied on the grouped dataset.

#### Parameters

<Expandable title="aggregates" type="List[Aggregation]">

Positional argument specifying the list of aggregations to apply on the grouped
dataset. This list can be passed either as an explicit list or just as unpacked 
*args.
</Expandable>
See [aggregations](/api-reference/aggregations) for the full list of aggregate functions.
</LeftSection>

<RightSection>
<pre snippet="api-reference/operators_ref#aggregate"></pre>
</RightSection>

</Divider>

