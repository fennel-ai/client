---
title: Distinct
order: 0
status: published
---
### Distinct

<Divider>
<LeftSection>

Aggregation to computes a set of distinct values for each group within a window. 

#### Parameters
<Expandable title="of" type="str">
Name of the field in the input dataset over which the distinct set should be 
computed.  This field must be of any hashable type (e.g. floats aren't allowed)
</Expandable>

<Expandable title="window" type="Window">
The continuous window within which aggregation needs to be computed. Possible 
values are `"forever"` or any [time duration](/api-reference/data-types/duration).
</Expandable>

<Expandable title="into_field" type="str">
The name of the field in the output dataset that should store the result of this
aggregation. This field is expected to be of type `List[T]` where `T` is the type
of the field denoted by `of`.
</Expandable>

<Expandable title="unordered" type="float">
If set to True, the list is sorted by natural comparison order. However, as of 
right now, this must be set to False since `ordered` mode isn't supported yet.
</Expandable>

#### Returns
<Expandable type="List[T]">
Stores the result of the aggregation in the appropriate column of the output 
dataset which must be of type `List[T]` where `T` is the type of the input column.
</Expandable>


#### Errors
<Expandable title="Computing distinct for non-hashable types">
Distinct operator is a lot like building a hashmap - for it to be valid, the 
underlying data must be hashable. Types like `float` (or any other complex type
built using `float`) aren't hashable - so a sync error is raised.

</Expandable>

:::warning
Storing the full set of distinct values can get costly so it's recommended to use
`Distinct` only for sets of small cardinality (say < 100)
:::

</LeftSection>
<RightSection>
<pre snippet="api-reference/aggregations/distinct#basic" status="success" 
    message="Distinct in window of 1 day">
</pre>
<pre snippet="api-reference/aggregations/distinct#incorrect_type" status="error" 
    message="amounts should be of type List[int], not int">
</pre>
</RightSection>
</Divider>
