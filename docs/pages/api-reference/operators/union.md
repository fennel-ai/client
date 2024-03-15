---
title: Union
order: 0
status: published
---
### Union

<Divider>
<LeftSection>
Operator to union rows from two datasets of the identical schema. Only 
applicable to keyless datasets. Written as simple `+` operator on two datasets.

#### Returns
<Expandable type="Dataset">
Returns a dataset with the same schema as both the input datasets but containing
rows from both of them. If both contain the identical row, two copies of that row
are present in the output datasets.
</Expandable>


#### Errors
<Expandable title="Taking union of datasets with different schemas">
Union operator is defined only when both the input datasets have the same
schema. Commit error to apply union on input datasets with different schemas.
</Expandable>

<Expandable title="Taking union of keyed datasets">
Commit error to apply union on input datasets with key columns.
</Expandable>

</LeftSection>

<RightSection>
<pre snippet="api-reference/operators/union#basic" status="success"
    message="Union an s3 and kafka dataset">
</pre>

<pre snippet="api-reference/operators/explode#exploding_non_list" status="error"
    message="Exploding a non-list column" highlight="5, 17">
</pre>

<pre snippet="api-reference/operators/explode#exploding_missing" status="error"
    message="Exploding a non-existent column" highlight="17">
</pre>

</RightSection>

</Divider>