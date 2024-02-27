---
title: Rename
order: 0
status: published
---
### Rename

<Divider>
<LeftSection>
Operator to rename columns of a dataset.

#### Parameters

<Expandable title="columns" type="Dict[str, str]">
Dictionary mapping from old column names to their new names.

All columns should still have distinct and valid names post renaming.

</Expandable>

#### Returns
<Expandable type="Dataset">
Returns a dataset with the same schema as the input dataset, just with the 
columns renamed.
</Expandable>

#### Errors
<Expandable title="Renaming non-existent column">
Commit error if there is no existing column with name matching each of the keys
in the rename dictionary.
</Expandable>

<Expandable title="Conflicting column names post-rename">
Commit error if after renaming, there will be two columns in the dataset having
the same name.
</Expandable>

</LeftSection>



<RightSection>
    <pre snippet="api-reference/operators/rename#basic"
        status="success" message="Rename weight -> weight_lb & height -> height_in"
    ></pre>
</RightSection>

</Divider>

