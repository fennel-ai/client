---
title: Select
order: 0
status: published
---
# Select

<Divider>
<LeftSection>
Operator to select some columns from a dataset.

#### Parameters

<Expandable title="columns" type="List[str]">
List of columns in the incoming dataset that should be selected into the output
dataset. This can be passed either as unpacked *args or as kwarg set to a Python 
list.

For keyed datasets, all key fields must always be selected. Timestamp field is
automatically included whether provided in select or not.

</Expandable>
</LeftSection>


<RightSection>
<pre snippet="api-reference/operators_ref#select"></pre>
</RightSection>

</Divider>

TODO:
- one example with *args, one with list
- one invalid example where skipping a key field

