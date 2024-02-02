---
title: First
order: 0
status: published
---
# First

<Divider>
<LeftSection>
Operator to find the first element of a group by the row timestamp. First 
operator must always be preceded by a [groupby](/api-reference/operators/groupby) 
operator. For each group formed by grouping, one row is chosen having the lowest
value in the timestamp field. In case of ties, the first seen row wins.

The `first` operator does not take any parameters.

#### Returns
The returned dataset's fields are the same as the input dataset, with the 
grouping fields as the keys.

</LeftSection>

<RightSection>
<pre snippet="api-reference/operators_ref#first"></pre>
</RightSection>

</Divider>



