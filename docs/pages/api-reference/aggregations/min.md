---
title: Min
order: 0
status: published
---
### Min
Aggregation to computes a rolling min for each group within a window. 

#### Parameters
<Expandable title="of" type="str">
Name of the field in the input dataset over which the min should be computed. 
This field must either be of type `int`, `float`, `date` or `datetime`.
</Expandable>

<Expandable title="window" type="Window">
The continuous window within which aggregation needs to be computed. Possible 
values are `"forever"` or any [time duration](/api-reference/data-types/duration).
</Expandable>

<Expandable title="into_field" type="str">
The name of the field in the output dataset that should store the result of this
aggregation. This field is expected to be of type `int`, `float`, `date` or
`datetime` - same as the type of the field in the input dataset corresponding to
`of`.
</Expandable>

<Expandable title="default" type="Optional[Union[int, float, Decimal, datetime, date]]">
Min over an empty set of rows isn't well defined - Fennel returns `default`
in such cases. The type of `default` must be same as that of `of` in the input
dataset. If the default is not set or is None, Fennel returns None and in that case, 
the expected type of `into_field` must be `Optional[T]`.
</Expandable>

<pre snippet="api-reference/aggregations/min#basic" status="success" 
    message="Minimum in rolling window of 1 day & 1 week">
</pre>

#### Returns
<Expandable type="Union[int, float, date, datetime]">
Stores the result of the aggregation in the appropriate field of the output 
dataset. If there are no rows in the aggregation window, `default` is used.
</Expandable>


#### Errors
<Expandable title="Min on other types">
The input column denoted by `of` must be of `int`, `float`, `decimal`, 
`date` or `datetime` types. 

Note that like SQL, aggregations over `Optional[int]` or `Optional[float]` 
are allowed.
</Expandable>

<Expandable title="Types of input, output & default don't match">
The type of the field denoted by `into_field` in the output dataset and that of
`default` should be same as that of the field field denoted by `of` in the 
input dataset.
</Expandable>

<pre snippet="api-reference/aggregations/min#incorrect_type" status="error" 
    message="Can not take min over string; only int, float, date or datetime">
</pre>
<pre snippet="api-reference/aggregations/min#non_matching_types" status="error" 
    message="amt is float but min_1d is int">
</pre>
