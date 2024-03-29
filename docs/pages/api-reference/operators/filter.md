---
title: Filter
order: 0
status: published
---

### Filter
Operator to selectively `filter` out rows from a dataset. 

#### Parameters
<Expandable title="func" type="Callable[pd.Dataframe, pd.Series[bool]]">

The actual filter function - takes a pandas dataframe containing a batch of rows 
from the input dataset and is expected to return a series of booleans of the 
same length. Only rows corresponding to `True` are retained in the output dataset.
</Expandable>

<pre snippet="api-reference/operators/filter#basic" status="success" 
   message="Filtering out rows where city is London" highlight="23">
</pre>

#### Returns
<Expandable type="Dataset">
Returns a dataset with the same schema as the input dataset, just with some rows
potentially filtered out.
</Expandable>


#### Errors
<Expandable title="Invalid series at runtime">
Runtime error if the value returned from the lambda isn't a pandas Series of
the bool and of the same length as the input dataframe.
</Expandable>

<pre snippet="api-reference/operators/filter#incorrect_type" status="error" 
   message="Runtime Error: Lambda returns str, not bool" highlight="23">
</pre>
