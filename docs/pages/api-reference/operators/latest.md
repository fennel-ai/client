---
title: Latest 
order: 0
status: published
---
### Latest
Operator to find the latest element of a group by the row timestamp. Latest 
operator must always be preceded by a [groupby](/api-reference/operators/groupby) 
operator. 

#### Parameters
The `latest` operator does not take any parameters.

<pre snippet="api-reference/operators/latest#basic" status="success"
    message="Dataset with just the latest transaction of each user">
</pre>

#### Returns
<Expandable type="Dataset">
The returned dataset's fields are the same as the input dataset, with the 
grouping fields as the keys.  

The row with the maximum timestamp is chosen for each group. 
The last seen row wins in case of ties
</Expandable>

