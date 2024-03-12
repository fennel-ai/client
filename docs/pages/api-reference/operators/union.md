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
<Expandable title="Unioning datasets with different schemas">
Union operator is defined only when both the input datasets have the same
schema. Commit error to apply union on input datasets with different schemas.
</Expandable>

<Expandable title="Unioning keyed datasets">
Commit error to apply union on input datasets with key columns.
</Expandable>

</LeftSection>

<RightSection>
<pre snippet="api-reference/operators/union#basic" status="success"
    message="Union an s3 and kafka dataset">
</pre>
</RightSection>

</Divider>