---
title: Assign
order: 0
status: published
---
# Assign

<Divider>
<LeftSection>
Operator to add a new column to a dataset - the added column is neither a key column
nor a timestamp column.


#### Parameters

<Expandable title="name" type="str">
The name of the new column to be added - must not conflict with any existing name
on the dataset.
</Expandable>

<Expandable title="dtype" type="Type">
The data type of the new column to be added - must be a valid Fennel supported
[data type](/api-reference#api-reference/data-types).
</Expandable>

<Expandable title="func" type="Callable[pd.Dataframe, pd.Series[T]]">
The function, which when given a subset of the dataset as a dataframe, returns
the value of the new column for each row in the dataframe. Fennel verifies
at runtime that the returned series matches the declared `dtype`.
</Expandable>


</LeftSection>

<RightSection>
<pre snippet="api-reference/operators_ref#assign"></pre>
</RightSection>

</Divider>

TODO:
- one valid case
- invalid - returns incorrect dtype, name conflict