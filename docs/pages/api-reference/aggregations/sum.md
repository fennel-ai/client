---
title: Sum
order: 0
status: published
---
### Sum
Aggregation to compute a rolling sum for each group within a window. 

#### Parameters
<Expandable title="of" type="str">
Name of the field in the input dataset over which the sum should be computed. 
This field can only either be `int` or `float.
</Expandable>

<Expandable title="window" type="Window">
The continuous window within which something need to be counted. Possible values
are `"forever"` or any [time duration](/api-reference/data-types/duration).
</Expandable>

<Expandable title="into_field" type="str">
The name of the field in the output dataset that should store the result of this
aggregation. This field is expected to be of type `int` or `float` - same as the
type of the field in the input dataset corresponding to `of`.
</Expandable>

<pre snippet="api-reference/aggregations/sum#basic" status="success" 
    message="Sum up amount in 1 week and forever windows">
</pre>

#### Returns
<Expandable type="Union[int, float]">
Accumulates the count in the appropriate field of the output dataset. If there 
are no rows to count, by default, it returns 0 (or 0.0 if `of` is float).
</Expandable>


#### Errors
<Expandable title="Sum on non int/float types">
The input column denoted by `of` must either be of `int` or `float` types. 

Note that unlike SQL, even aggregations over `Optional[int]` or `Optional[float]` 
aren't allowed.
</Expandable>

<pre snippet="api-reference/aggregations/sum#incorrect_type" status="error" 
    message="Can only sum up int or float types">
</pre>
