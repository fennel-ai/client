---
title: Assign
order: 0
status: published
---
### Assign

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
the value of the new column for each row in the dataframe. 

Fennel verifies at runtime that the returned series matches the declared `dtype`.
</Expandable>


#### Returns
<Expandable type="Dataset">
Returns a dataset with one additional column of the given `name` and type same
as `dtype`. This additional column is neither a key-column or the timestamp 
column.
</Expandable>


#### Errors
<Expandable title="Invalid series at runtime">
Runtime error if the value returned from the lambda isn't a pandas Series of
the declared type and the same length as the input dataframe.
</Expandable>

</LeftSection>

<RightSection>
<pre snippet="api-reference/operators/assign#basic" status="success" 
    message="Adding new column 'amount_sq' of type int" highlight="17, 23"
>
</pre>
<pre snippet="api-reference/operators/assign#incorrect_type" status="error" 
    message="Runtime error: returns float, not int" highlight="17, 23"
>
</pre>
</RightSection>

</Divider>