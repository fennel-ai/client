---
title: Transform
order: 0
status: published
---
# Transform

<Divider>
<LeftSection>
Catch all operator to add/remove/update columns.

#### Parameters


<Expandable title="func" type="Callable[pd.Dataframe, pd.Dataframe]">
Positional argument specifying the transform function - the function takes a 
pandas dataframe containing a batch of rows from the input dataset and is 
expected to return an output dataframe of the same length, though potentially
with different set of columns.
</Expandable>

<Expandable title="schema" type="Optional[Dict[str, Type]]" default="None">
The expected schema of the output dataset. If not specified, the schema of the
input dataset is used.
</Expandable>

:::info
If possible, it's recommended to use simpler operators like assign, drop, 
select, dropnull, rename since they are easier to read/write.
:::
</LeftSection>

<RightSection>
<pre snippet="api-reference/operators_ref#transform"></pre>
</RightSection>

</Divider>