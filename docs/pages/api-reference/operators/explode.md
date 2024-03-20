---
title: Explode
order: 0
status: published
---
### Explode
Operator to explode lists in a single row to form multiple rows, analogous to 
to the `explode`function in [Pandas](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.explode.html).

Only applicable to keyless datasets.

#### Parameters

<Expandable title="columns" type="List[str]">
The list of columns to explode. This list can be passed either as 
unpacked *args or kwarg `columns` mapping to an explicit list.

All the columns should be of type `List[T]` for some `T` in the input dataset and
after explosion, they get converted to a column of type `Optional[T]`.
</Expandable>

<pre snippet="api-reference/operators/explode#basic" status="success"
    message="Exploding skus and prices together" highlight="19">
</pre>

#### Returns
<Expandable type="Dataset">
Returns a dataset with the same number & name of columns as the input dataset but
with the type of exploded columns modified from `List[T]` to `Optional[T]`. 

Empty lists are converted to `None` values (hence the output types need to be
`Optional[T]`).
</Expandable>


#### Errors
<Expandable title="Exploding keyed datasets">
Sync error to apply explode on an input dataset with key columns.
</Expandable>

<Expandable title="Exploding non-list columns">
Sync error to explode using a column that is not of the type `List[T]`.
</Expandable>

<Expandable title="Exploding non-existent columns">
Sync error to explode using a column that is not present in the input dataset.
</Expandable>

<Expandable title="Unequal size lists in multi-column explode">
For a given row, all the columns getting exploded must have lists of the same
length, otherwise a runtime error is raised. Note that the lists can be of 
different type across rows.
</Expandable>

<pre snippet="api-reference/operators/explode#exploding_non_list" status="error"
    message="Exploding a non-list column" highlight="5, 17">
</pre>

<pre snippet="api-reference/operators/explode#exploding_missing" status="error"
    message="Exploding a non-existent column" highlight="17">
</pre>
