---
title: Quantile
order: 0
status: published
---
### Quantile
Aggregation to compute rolling quantiles (aka percentiles) for each group 
within a window. 

#### Parameters
<Expandable title="of" type="str">
Name of the field in the input dataset over which the quantile should be computed.
This field must either be of type `int` or `float`.
</Expandable>

<Expandable title="window" type="Window">
The continuous window within which aggregation needs to be computed. Possible 
values are `"forever"` or any [time duration](/api-reference/data-types/duration).
</Expandable>

<Expandable title="into_field" type="str">
The name of the field in the output dataset that should store the result of this
aggregation. This field is expected to be of type `Optional[float]` unless `default`
is provided, in which case, it is expected to be of type `float`.
</Expandable>

<Expandable title="default" type="Optional[float]">
Quantile over an empty set of rows isn't well defined - Fennel returns `default`
in such cases. If the default is not set or is None, Fennel returns None and in
that case, the expected type of `into_field` must be `Optional[float]`.
</Expandable>


<Expandable title="p" type="float">
The percentile (between 0 and 1) to be calculated.
</Expandable>

<Expandable title="approx" type="bool" defaultVal="False">
If set to True, the calculated value isn't exact but only an approximation. Fennel
only supports approximate quantiles for now so this kwarg must always be set to
True.

Fennel uses [uDDsketch](https://arxiv.org/pdf/2004.08604.pdf) data 
structure to compute approximate quantiles with an error bound set to be within
1% of the true value.
</Expandable>

<pre snippet="api-reference/aggregations/quantile#basic" status="success" 
    message="Median in rolling windows of 1 day & 1 week">
</pre>

#### Returns
<Expandable type="Union[float, Optional[float]]">
Stores the result of the aggregation in the appropriate field of the output 
dataset. If there are no rows in the aggregation window, `default` is used.
</Expandable>


#### Errors
<Expandable title="Quantile on non int/float types">
The input column denoted by `of` must either be of `int` or `float` or 
`decimal` types.

Note that like SQL, aggregations over `Optional[int]` or `Optional[float]` 
are allowed.
</Expandable>

<Expandable title="Types of output & default don't match">
The type of the field denoted by `into_field` in the output dataset should match 
the `default`. If `default` is set and not None, the field should be `float` else
it should be `Optional[float]`.
</Expandable>

<Expandable title="Invalid p value">
Commit error if the value of `p` is not between 0 and 1.
</Expandable>

<Expandable title="Approximate is not set to true">
Commit error if `approx` is not set to True. Fennel only supports approximate
quantiles for now but requires this kwarg to be set explicitly to both set the
right expectations and be compatible with future addition of exact quantiles.
</Expandable>

<pre snippet="api-reference/aggregations/quantile#incorrect_type" status="error" 
    message="Can not take quantile over string, only int or float or decimal">
</pre>
<pre snippet="api-reference/aggregations/quantile#invalid_default" status="error" 
    message="Default is not specified, so the output field should be Optional[float]">
</pre>
<pre snippet="api-reference/aggregations/quantile#incorrect_p" status="error" 
    message="p is invalid, can only be between [0, 1]">
</pre>
