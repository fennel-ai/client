---
title: First
order: 0
status: published
---
### First
Operator to find the first element of a group by the row timestamp. First 
operator must always be preceded by a [groupby](/api-reference/operators/groupby) 
operator. 

#### Parameters
The `first` operator does not take any parameters.

<pre snippet="api-reference/operators/first#basic" status="success"
    message="Dataset with just the first transaction of each user">
</pre>

#### Returns
<Expandable type="Dataset">
The returned dataset's fields are the same as the input dataset, with the 
grouping fields as the keys.  

For each group formed by grouping, one row is chosen having the lowest
value in the timestamp field. In case of ties, the first seen row wins.
</Expandable>

