---
title: Filter
order: 0
status: published
---

# Filter

<Divider>
<LeftSection>
Operator to selectively `filter` out rows from a dataset. 

#### Parameters


<Expandable title="func" type="Callable[pd.Dataframe, pd.Series[bool]]">

Positional argument specifying the filter function - the function takes a pandas
dataframe containing a batch of rows from the input dataset and is expected to 
return a series of booleans of the same length. Only rows corresponding to `True`
are retained in the output dataset.
</Expandable>

</LeftSection>

<RightSection>
<pre snippet="api-reference/operators_ref#filter"></pre>
</RightSection>

</Divider>
