---
title: Dropnull
order: 0
status: published
---
### Dropnull
Operator to drop rows containing null values (aka None in Python speak) in
the given columns.

#### Parameters
<Expandable title="columns" type="Optional[List[str]]">
List of columns in the incoming dataset that should be checked for presence of 
None values - if any such column has None for a row, the row will be filtered out
from the output dataset. This can be passed either as unpacked *args or as a 
Python list.

If no arguments are given, `columns` will be all columns with the type `Optional[T]` 
in the dataset.

</Expandable>

<pre snippet="api-reference/operators/dropnull#basic" status="success"
    message="Dropnull on city & country, but not gender">
</pre>
<pre snippet="api-reference/operators/dropnull#dropnull_all" status="success"
    message="Applies to all optional columns if none is given explicitly"
    >
</pre>

#### Returns
<Expandable type="Dataset">
Returns a dataset with the same name & number of columns as the input dataset but
with the type of some columns modified from `Optional[T]` -> `T`.
</Expandable>

#### Errors
<Expandable title="Dropnull on non-optional columns">
Commit error to pass a column without an optional type.
</Expandable>

<Expandable title="Dropnull on non-existent columns">
Commit error to pass a column that doesn't exist in the input dataset.
</Expandable>

<pre snippet="api-reference/operators/dropnull#missing_column" status="error"
    message="Dropnull on a non-existent column" >
</pre>

<pre snippet="api-reference/operators/dropnull#non_optional_column" status="error"
    message="Dropnull on a non-optional column">
</pre>
