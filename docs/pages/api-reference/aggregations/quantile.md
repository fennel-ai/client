---
title: Quantile
order: 0
status: published
---
### Quantile

<Divider>
<LeftSection>

Aggregation to computes a rolling quantile for each group within a window. 

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
aggregation. This field is expected to be of type `float` or `Optional[float]` depends on if the default is set
</Expandable>

<Expandable title="default" type="Optional[float]">
Quantile over an empty set of rows isn't well defined - Fennel returns `default`
in such cases. If the default is not set or None, the expected type of `into_field` is `Optional[float]`
</Expandable>


<Expandable title="p" type="float">
The percentile to calculate over the aggregate data. Valid values are between 0 and 1.
</Expandable>

#### Returns
<Expandable type="Union[float, Optional[float]]">
Stores the result of the aggregation in the appropriate field of the output 
dataset. If there are no rows in the aggregation window, `default` is used.
</Expandable>


<Expandable title="approx" type="bool" defaultVal="False">
If set to True, the count isn't exact but only an approximation. This field must
be set to True.

Fennel uses [uDDsketch](https://arxiv.org/pdf/2004.08604.pdf) data 
structure to compute approximate quantiles. The error percentage is less than 1%.
</Expandable>


#### Errors
<Expandable title="Quantile on non int/float types">
The input column denoted by `of` must either be of `int` or `float` types. 

Note that unlike SQL, even aggregations over `Optional[int]` or `Optional[float]` 
aren't allowed.
</Expandable>

<Expandable title="Types of output & default don't match">
The type of the field denoted by `into_field` in the output dataset should match the
`default`. If `default` is set and not None the field should be `Optional[float]`
</Expandable>


<Expandable title="Invalid p value">
If the value of `p` is not between 0 and 1, it's an input error.
</Expandable>


<Expandable title="Approximate is not set to true">
Since Fennel used an approximate algorithm to save memory if the approximate flag is not set there will be an error, it's an input error.
</Expandable>



</LeftSection>
<RightSection>

<pre snippet="api-reference/aggregations/quantile#basic" status="success" 
    message="50 percentile in rolling window of 1 day & 1 week">
</pre>
<pre snippet="api-reference/aggregations/quantile#incorrect_type" status="error" 
    message="Can not take quantile over string, only int or float">
</pre>
<pre snippet="api-reference/aggregations/quantile#invalid_default" status="error" 
    message="Default is not specify so the output field should be Optional[float]">
</pre>
<pre snippet="api-reference/aggregations/quantile#no_approx" status="error" 
    message="approx is not set">
</pre>
<pre snippet="api-reference/aggregations/quantile#incorrect_p" status="error" 
    message="p is a invalid value">
</pre>

</RightSection>
</Divider>
