---
title: Count
order: 0
status: published
---
### Count
Aggregation to compute a rolling count for each group within a window. 

#### Parameters
<Expandable title="window" type="Window">
The continuous window within which something need to be counted. Possible values
are `"forever"` or any [time duration](/api-reference/data-types/duration).
</Expandable>

<Expandable title="into_field" type="str">
The name of the field in the output dataset that should store the result of this
aggregation. This field is expected to be of type `int`.
</Expandable>

<Expandable title="unique" type="bool" defaultVal="False">
If set to True, the aggregation counts the number of unique values of the field
given by `of` (aka `COUNT DISTINCT` in SQL). 
</Expandable>

<Expandable title="approx" type="bool" defaultVal="False">
If set to True, the count isn't exact but only an approximation. This field must
be set to True if and only if `unique` is set to True. 

Fennel uses [hyperloglog](https://en.wikipedia.org/wiki/HyperLogLog) data 
structure to compute unique approximate counts and in practice, the count is 
exact for small counts.
</Expandable>

<Expandable title="of" type="Optional[str]">
Name of the field in the input dataset which should be used for `unique`. Only 
relevant when `unique` is set to True. 
</Expandable>

<pre snippet="api-reference/aggregations/count#basic" status="success" 
    message="Count # of transaction & distinct vendors per user">
</pre>

#### Returns
<Expandable type="int">
Accumulates the count in the appropriate field of the output dataset. If there 
are no rows to count, by default, it returns 0.
</Expandable>

#### Errors
<Expandable title="Count unique on unhashable type">
The input column denoted by `of` must have a hashable type in order to build a
hyperloglog. For instance, `float` or types built on `float` aren't allowed.
</Expandable>

<Expandable title="Unique counts without approx">
As of right now, it's a sync error to try to compute unique count without setting
`approx` to True.
</Expandable>

:::warning
Maintaining unique counts is substantially more costly than maintaining 
non-unique counts so use it only when truly needed.
:::

