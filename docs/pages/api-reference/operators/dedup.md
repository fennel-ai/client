---
title: Dedup
order: 0
status: published
---
# Dedup

<Divider>
<LeftSection>
Operator to dedup keyless datasets (e.g. event streams).

#### Parameters

<Expandable title="by" type="Optional[List[str]]">

The list of columns to use for identifying duplicates. If not specified, all 
the columns are used for identifying duplicates.

Two rows of the input dataset are considered duplicates if and only if they have 
the same values for the timestamp column and all the `by` columns.

</Expandable>
</LeftSection>

<RightSection>
</RightSection>

</Divider>

