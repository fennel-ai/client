---
title: LastK
order: 0
status: published
---
### LastK

<Divider>
<LeftSection>

Aggregation to computes a rolling list of the latest values for each group 
within a window. 

#### Parameters
<Expandable title="of" type="str">
Name of the field in the input dataset over which the aggregation should be 
computed.
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

<Expandable title="limit" type="int">
Since storing all the values for a group can get costly, LastK expects a 
`limit` to be specified which denotes the maximum size of the list that should 
be maintained at any point.
</Expandable>

<Expandable title="dedup" type="bool">
If set to True, only distinct values are stored else values stored in the last
can have duplicates too.
</Expandable>

#### Returns
<Expandable type="List[T]">
Stores the result of the aggregation in the appropriate field of the output 
dataset. 
</Expandable>


#### Errors
<Expandable title="Incorrect output type">
The column denoted by `into_field` in the output dataset must be of type `List[T]`
where T is the type of the column denoted by `of` in the input dataset. Sync error
is raised if this is not the case.
</Expandable>

:::warning
Storing the full set of values and maintaining order between them can get costly, 
so use this aggregation only when needed.
:::

</LeftSection>
<RightSection>
<pre snippet="api-reference/aggregations/lastk#basic" status="success" 
    message="LastK in window of 1 day">
</pre>
<pre snippet="api-reference/aggregations/lastk#incorrect_type" status="error" 
    message="amounts should be of type List[int], not int">
</pre>

</RightSection>
</Divider>
