---
title: Average
order: 0
status: published
---
### Average
Aggregation to computes a rolling average for each group within a window. 

#### Parameters
<Expandable title="of" type="str">
Name of the field in the input dataset over which the average should be computed.
This field must either be of type `int` or `float`.
</Expandable>

<Expandable title="window" type="Window">
The continuous window within which aggregation needs to be computed. Possible 
values are `"forever"` or any [time duration](/api-reference/data-types/duration).
</Expandable>

<Expandable title="into_field" type="str">
The name of the field in the output dataset that should store the result of this
aggregation. This field is expected to be of type `float`.
</Expandable>

<Expandable title="default" type="Optional[float]">
Average over an empty set of rows isn't well defined - Fennel returns `default`
in such cases. If the default is not set or is None, Fennel returns None and 
in that case, the expected type of `into_field` must be `Optional[float]`.
</Expandable>

<pre snippet="api-reference/aggregations/avg#basic" status="success" 
    message="Average in rolling window of 1 day & 1 week">
</pre>

#### Returns
<Expandable type="Union[float, Optional[float]]">
Stores the result of the aggregation in the appropriate field of the output 
dataset. If there are no rows in the aggregation window, `default` is used.
</Expandable>


#### Errors
<Expandable title="Average on non int/float/decimal types">
The input column denoted by `of` must either be of `int` or `float` or 
`decimal` types.

Note that like SQL, aggregations over `Optional[int]` or `Optional[float]` 
are allowed.
</Expandable>

<Expandable title="Output and/or default aren't float">
The type of the field denoted by `into_field` in the output dataset and that of
`default` should both be `float`.
</Expandable>

<pre snippet="api-reference/aggregations/avg#incorrect_type" status="error" 
    message="Can not take average over string, only int or float or decimal">
</pre>
<pre snippet="api-reference/aggregations/avg#non_matching_types" status="error" 
    message="Invalid type: ret is int but should be float">
</pre>