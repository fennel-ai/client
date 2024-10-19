---
title: Join
order: 0
status: published
---

### Join
Operator to join two datasets. The right hand side dataset must have 
one or more key columns and the join operation is performed on these columns.

#### Parameters

<Expandable title="dataset" type="Dataset">
The right hand side dataset to join this dataset with. RHS dataset
must be a keyed dataset and must also be an input to the pipeline (vs being an 
intermediary dataset derived within a pipeline itself).
</Expandable>

<Expandable title="how" type='"inner" | "left"'>
Required kwarg indicating whether the join should be an inner join (`how="inner"`)
or a left-outer join (`how="left"`). With `"left"`, the output dataset may have
a row even if there is no matching row on the right side. 
</Expandable>


<Expandable title="on" type="Optional[List[str]]" defaultVal="None">
Kwarg that specifies the list of fields along which join should happen. If present,
both left and right side datasets must have fields with these names and matching
data types (data types on left hand side can be optional). This list must be identical 
to the names of all key columns of the right hand side. 

If this isn't set, `left_on` and `right_on` must be set instead.
</Expandable>

<Expandable title="left_on" type="Optional[List[str]]" defaultVal="None">
Kwarg that specifies the list of fields from the left side dataset that should be
used for joining. If this kwarg is set, `right_on` must also be set. Note that
`right_on` must be identical to the names of all the key columns of the right side.
</Expandable>

<Expandable title="right_on" type="Optional[List[str]]" defaultVal="None">
Kwarg that specifies the list of fields from the right side dataset that should be
used for joining. If this kwarg is setup, `left_on` must also be set. The length
of `left_on` and `right_on` must be the same and corresponding fields on both 
sides must have the same data types.
</Expandable>

<Expandable title="within" type="Tuple[Duration, Duration]" defaultVal='("forever", "0s")'>
Optional kwarg specifying the time window relative to the left side timestamp 
within which the join should be performed. This can be seen as adding another
condition to join like `WHERE left_time - d1 < right_time AND right_time < left_time + d2`
where (d1, d2) = within.
- The first value in the tuple represents how far back in time should a join
   happen. The term "forever" means that we can go infinitely back in time 
   when searching for an event to join from the left-hand side data.
- The second value in the tuple represents how far ahead in time we can go to 
   perform a join. This is useful in cases when the corresponding RHS data of 
   the join can come later. The default value for this parameter is `("forever", 
   "0s")` which means that we can go infinitely back in time and the RHS data 
   should be available for the event time of the LHS data.
</Expandable>

<Expandable title="fields" type="Optional[List[str]]" defaultVal="None">
Optional kwarg that specifies the list of (non-key) fields of the right
dataset that should be included in the output dataset. If this kwarg is
not set, all such fields are included in the output dataset. If right dataset's
timestamp field is included in `fields`, then it is included as a normal field
in the output dataset, with left dataset's timestamp field as the output
dataset's timestamp field.
</Expandable>

<pre snippet="api-reference/operators/join#basic" status="success"
   message="Inner join on 'merchant'">
</pre>


<pre snippet="api-reference/operators/join#optional_join" status="success"
   message="Left join on 'merchant' with optional LHS fields">
</pre>

#### Returns
<Expandable type="Dataset">
Returns a dataset representing the joined dataset having the same keys & timestamp
columns as the LHS dataset. 

The output dataset has all the columns from the left dataset and all non-key 
non-timestamp columns from the right dataset.

If the join was of type `inner`, the type of a joined
RHS column of type `T` stays `T` but if the join was of type `left`, the type in
the output dataset becomes `Optional[T]` if it was `T` on the RHS side.

For LHS columns, the type is the same as the type in the LHS dataset if join type is `left`.
If the join type is `inner`, if a join column on the LHS is `Optional[T]`, then the type
in the output dataset is `T` (i.e., the `Optional` is dropped).
</Expandable>

#### Errors
<Expandable title="Join with non-key dataset on the right side">
Commit error to do a join with a dataset that doesn't have key columns.
</Expandable>

<Expandable title="Join with intermediate dataset">
Commit error to do a join with a dataset that is not an input to the pipeline but
instead is an intermediate dataset derived during the pipeline itself.
</Expandable>

<Expandable title="Post-join column name conflict">
Commit error if join will result in a dataset having two columns of the same name. 
A common way to work-around this is to rename columns via the [rename](/api-reference/operators/rename) operator before the join.
</Expandable>

<Expandable title="Mismatch in columns to be joined">
Commit error if the number/type of the join columns on the left and right side
don't match.
</Expandable>
