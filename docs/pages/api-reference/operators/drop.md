---
title: Drop
order: 0
status: published
---
# Drop

<Divider>
<LeftSection>
Operator to drop one or more non-key non-timestamp columns from a dataset.

#### Parameters

<Expandable title="columns" type="List[str]">
List of columns in the incoming dataset that should be dropped. This can be passed
either as unpacked *args or as a Python list.
</Expandable>

:::info
It is invalid to drop key or timestamp columns.
:::
</LeftSection>


<RightSection>
<pre snippet="api-reference/operators_ref#drop"></pre>
</RightSection>

</Divider>

TODO:
- one example with *args, one with list
- one invalid example where using a column that doesn't exist
- one invalid example when using key or timestamp column
