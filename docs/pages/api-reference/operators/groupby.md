---
title: Groupby
order: 0
status: published
---
# Groupby

<Divider>
<LeftSection>
Operator to group rows of incoming datasets to be processed by the next operator.

Technically, groupby isn't a standalone operator by itself since its output isn't
a valid dataset. Instead, it becomes a valid operator when followed by 
[first](/api-reference/operators/first), [aggregate](/api-reference/operators/aggregate), 
or [window](/api-reference/operators/window) operators.

#### Parameters

<Expandable title="keys" type="List[str]">

List of keys in the incoming dataset along which the rows should be grouped. This
can be passed as unpacked *args or a Python list.
</Expandable>

</LeftSection>

</Divider>

TODO:
- one example with *args, one with list
- one invalid example where using a field that doesn't exist
- one invalid example when including timestamp in groupby
