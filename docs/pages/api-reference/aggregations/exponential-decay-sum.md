---
title: Exponential Decay Sum
order: 0
status: published
---
### Exponential Decay Sum
Aggregation to compute a rolling exponential decay for each group within a window. 

#### Parameters
<Expandable title="of" type="str">
Name of the field in the input dataset over which the decayed sum should be computed. 
This field can only either be `int` or `float.
</Expandable>

<Expandable title="window" type="Window">
The continuous window within which something need to be counted. Possible values
are `"forever"` or any [time duration](/api-reference/data-types/duration).
</Expandable>

<Expandable title="half_life" type="Duration">
Half-life of the exponential decay. This is the time it takes for the value to
decay to half of its original value. The value of `half_life` must be greater than
0. The value is of type [duration](/api-reference/data-types/duration).

</Expandable>

<Expandable title="into_field" type="str">
The name of the field in the output dataset that should store the result of this
aggregation. This field is expected to be of type `int` or `float` - same as the
type of the field in the input dataset corresponding to `of`.
</Expandable>

<pre snippet="api-reference/aggregations/exp-decay-sum#basic" status="success" 
    message="Exponential decayed sum of up amount in 1 week and forever windows for different half lives">
</pre>

#### Returns
<Expandable type="float">
Accumulates the result in the appropriate field of the output dataset. If there 
are no rows to count, by default, it returns 0.0 
</Expandable>


#### Errors
<Expandable title="Sum on non int/float types">
The input column denoted by `of` must either be of `int` or `float` types. 
The output field denoted by `into_field` must always be of type `float`.

Note that unlike SQL, even aggregations over `Optional[int]` or `Optional[float]` 
aren't allowed.
</Expandable>

<pre snippet="api-reference/aggregations/exp-decay-sum#incorrect_type_exp_decay" status="error" 
    message="Output type should always be float">
</pre>


:::info
 The value of the decayed sum depends on the time you query the dataset. Hence it varies with request time for the same key
 in a dataset. Therefore pipelines containing this aggregation are terminal - no other operator can follow it. 
:::