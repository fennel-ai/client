---
title: Max
order: 0
status: published
---
### Max
Aggregation to computes a rolling max for each group within a window. 

#### Parameters
<Expandable title="of" type="str">
Name of the field in the input dataset over which the max should be computed.
This field must either be of type `int` or `float`.
</Expandable>

<Expandable title="window" type="Window">
The continuous window within which aggregation needs to be computed. Possible 
values are `"forever"` or any [time duration](/api-reference/data-types/duration).
</Expandable>

<Expandable title="into_field" type="str">
The name of the field in the output dataset that should store the result of this
aggregation. This field is expected to be of type `int` or `float` - same as the
type of the field in the input dataset corresponding to `of`.
</Expandable>

<Expandable title="default" type="Union[int, float]">
Max over an empty set of rows isn't well defined - Fennel returns `default`
in such cases. The type of `default` must be same as that of `of` in the input
dataset.
</Expandable>

<pre snippet="api-reference/aggregations/max#basic" status="success" 
    message="Maximum in rolling window of 1 day & 1 week">
</pre>

#### Returns
<Expandable type="Union[int, float]">
Stores the result of the aggregation in the appropriate field of the output 
dataset. If there are no rows in the aggregation window, `default` is used.
</Expandable>


#### Errors
<Expandable title="Max on non int/float types">
The input column denoted by `of` must either be of `int` or `float` types. 

Note that unlike SQL, even aggregations over `Optional[int]` or `Optional[float]` 
aren't allowed.
</Expandable>

<Expandable title="Types of input, output & default don't match">
The type of the field denoted by `into_field` in the output dataset and that of
`default` should be same as that of the field field denoted by `of` in the 
input dataset.
</Expandable>

<pre snippet="api-reference/aggregations/max#incorrect_type" status="error" 
    message="Can not take max over string, only int or float">
</pre>
<pre snippet="api-reference/aggregations/max#non_matching_types" status="error" 
    message="amt is float but max_1d is int">
</pre>