---
title: Assign
order: 0
status: published
---
### Assign
Operator to add a new column to a dataset - the added column is neither a key column
nor a timestamp column.


#### Parameters
<Expandable title="name" type="str">
The name of the new column to be added - must not conflict with any existing name
on the dataset.
</Expandable>

<Expandable title="dtype" type="Type">
The data type of the new column to be added - must be a valid Fennel supported
[data type](/api-reference/data-types).
</Expandable>

<Expandable title="func" type="Callable[pd.Dataframe, pd.Series[T]]">
The function, which when given a subset of the dataset as a dataframe, returns
the value of the new column for each row in the dataframe. 

Fennel verifies at runtime that the returned series matches the declared `dtype`.
</Expandable>

<Expandable title="**kwargs" type="TypedExpression">
Assign can also be given one or more expressions instead of Python lambdas - it
can either have expressions or lambdas but not both. Expected types must also be
present along with each expression (see example). 

Unlike lambda based assign, all type validation and many other errors can be 
verified at the commit time itself (vs incurring runtime errors).
</Expandable>

<pre snippet="api-reference/operators/assign#basic" status="success" 
    message="Adding new column 'amount_sq' of type int" highlight="18, 24"
>
</pre>

<pre snippet="api-reference/operators/assign#expression" status="success" 
    message="Adding two new columns using expressions"
>
</pre>

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

<Expandable title="Invalid expression at import/commit time">
When using expressions, errors may be raised during the import or commit if types 
don't match and/or there are other validation errors related to the expressions.
</Expandable>

<pre snippet="api-reference/operators/assign#incorrect_type" status="error" 
    message="Runtime error: returns float, not int">
</pre>

<pre snippet="api-reference/operators/assign#incorrect_type_expr" status="error" 
    message="Import error: age_half is expected to be int but expr evaluates to float"
> </pre>
