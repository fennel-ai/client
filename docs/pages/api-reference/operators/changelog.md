---
title: Changelog
order: 0
status: published
---
### Changelog
Operator to convert a keyed dataset into a keyless one, where the underlying delta
frame of the dataset is presented as a changelog. All key fields are converted into
normal fields, and an additional column is added which contains the kind (insert
or delete) of the delta.

#### Parameters

<Expandable title="delete" type="str">
Kwarg that specifies the name of a boolean column which stores whether a delta was
a delete kind in the original dataset. Exactly one of this or `insert` kwarg
should be set.
</Expandable>

<Expandable title="insert" type="str">
Kwarg that specifies the name of a boolean column which stores whether a delta was
an insert kind in the original dataset. Exactly one of this or `delete` kwarg
should be set.
</Expandable>

#### Returns

<Expandable type="Dataset">
Returns a dataset with underlying delta frame of the input dataset is presented as
a insert only changelog. All key fields converted into normal fields and an additional
column is added which contains the kind (insert or delete) of the delta.
</Expandable>

#### Errors
<Expandable title="Neither insert nor delete kwarg is set">
Error if neither of `insert` or `delete` kwarg is set.
</Expandable>

<Expandable title="Both insert and delete kwargs are set">
Error if both `insert` and `delete` kwargs are set.
</Expandable>
