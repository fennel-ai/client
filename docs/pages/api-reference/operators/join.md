---
title: Join
order: 0
status: published
---

# Join

<Divider>
<LeftSection>
Operator to join two datasets. The right hand side dataset of join must have 
keys defined and the join operation is performed on these keys of the RHS dataset.

#### Parameters

<Expandable title="dataset" type="datasets.Dataset">
Position argument that specifies the right hand side dataset to join. This dataset
must be a keyed dataset and must also be an input to the pipeline (vs being an 
intermediary dataset derived within a pipeline itself).
</Expandable>

<Expandable title="how" type='Literal["inner", "left"]'>
Required kwarg indicating whether the join should be an inner join (`how="inner"`)
or a left-outer join (`how="left"`). With `"left"`, the output dataset may have
a row even if there is no matching row on the right side. As a result, the
data type for a column on right with type `T` becomes `Option[T]` in the joined 
dataset. Such conversion doesn't happen with `"inner"`.
</Expandable>


<Expandable title="on" type="Optional[List[str]]" defaultVal="None">
Kwarg that specifies the list of fields along which join should happen. If present,
both left and right side datasets must have fields with these names and matching
data types. This list must be identical to the key fields of the right hand side. 

If this isn't set, `left_on` and `right_on` must be set instead.
</Expandable>

<Expandable title="left_on" type="Optional[List[str]]" defaultVal="None">
Kwarg that specifies the list of fields from the left side dataset that should be
used for joining. If this kwarg is setup, `right_on` must also be set. Note that
`right_on` must be identical to the names of all the key fields of the right side.
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
condition to join like `WHERE left_time - d1 < right_time AND right_time < left_time + d1`
1. The first value in the tuple represents how far back in time should a join
   happen. The term "forever" means that we can go infinitely back in time 
   when searching for an event to join from the left-hand side data.
2. The second value in the tuple represents how far ahead in time we can go to 
   perform a join. This is useful in cases when the corresponding RHS data of 
   the join can come later. The default value for this parameter is `("forever", 
   "0s")` which means that we can go infinitely back in time and the RHS data 
   should be available for the event time of the LHS data.
</Expandable>


</LeftSection>

<RightSection>
<pre snippet="api-reference/operators_ref#join"></pre>
</RightSection>

</Divider>
:::info
Fennel converts the schema of the RHS dataset to become 
optional during left-join operation. This is because the join operation could 
result in null values for the RHS dataset.
:::

:::info
Column name collisions during the join operation aren't permitted. To prevent
collisions, you may want to rename columns in the LHS OR RHS datasets before 
the join.
:::