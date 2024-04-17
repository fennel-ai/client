---
title: Latest 
order: 0
status: published
---
### Latest
Operator to find the latest element of a group by the row timestamp. Latest 
operator must always be preceded by a [groupby](/api-reference/operators/groupby) 
operator. 

Latest operator is a good way to effectively convert a stream of only append
to a time-aware upsert stream.

#### Parameters
The `latest` operator does not take any parameters.

<pre snippet="api-reference/operators/latest#basic" status="success"
    message="Dataset with just the latest transaction of each user">
</pre>

#### Returns
<Expandable type="Dataset">
The returned dataset's fields are the same as the input dataset, with the 
grouping fields as the keys.  

The row with the maximum timestamp is chosen for each group. In case of ties, 
the last seen row wins.
</Expandable>
