---
title: Dropnull
order: 0
status: published
---
# Dropnull

<Divider>
<LeftSection>
Operator to drop rows containing null values (i.e. None in Python speak) in
the given columns and change the type of the columns from `Optional[T]` to `T`

#### Parameters

<Expandable title="columns" type="Optional[List[str]]">
List of columns in the incoming dataset that should be checked for presence of 
None values - if any such column has None for a row, the row will be filtered out
from the output dataset. This can be passed either as unpacked *args or as a 
Python list.

If no arguments are given, `columns` will be all columns with the type `Optional[T]` 
in the dataset.

It's an error to pass a column without an optional type to dropnull.

</Expandable>
</LeftSection>


<RightSection>
<pre snippet="api-reference/operators_ref#dropnull"></pre>
</RightSection>

</Divider>

TODO:
- one example with *args, one with list
- one example without any name - defaults to all optional fields
- one invalid example where using a field that doesn't exist
- one invalid example when passing a non-optional field

