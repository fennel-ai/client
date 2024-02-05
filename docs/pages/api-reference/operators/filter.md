---
title: Filter
order: 0
status: published
---

### Filter

<Divider>
<LeftSection>
Operator to selectively `filter` out rows from a dataset. 

#### Parameters
<Expandable title="func" type="Callable[pd.Dataframe, pd.Series[bool]]">

The actual filter function - takes a pandas dataframe containing a batch of rows 
from the input dataset and is expected to return a series of booleans of the 
same length. Only rows corresponding to `True` are retained in the output dataset.
</Expandable>

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

</LeftSection>

<RightSection>
<pre snippet="api-reference/operators/filter#basic" status="success" 
   message="Filtering out rows where city is London">
</pre>
<pre snippet="api-reference/operators/filter#basic" status="error" 
   message="Runtime Error: Lambda returns str, not bool">
</pre>

</RightSection>

</Divider>
