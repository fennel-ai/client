---
title: Select
order: 0
status: published
---
### Select

<Divider>
<LeftSection>
Operator to select some columns from a dataset.

#### Parameters

<Expandable title="columns" type="List[str]">
List of columns in the incoming dataset that should be selected into the output
dataset. This can be passed either as unpacked *args or as kwarg set to a Python 
list.
</Expandable>

#### Returns
<Expandable type="Dataset">
Returns a dataset containing only the selected columns. Timestamp field is 
automatically included whether explicitly provided in the select or not.
</Expandable>

#### Errors
<Expandable title="Not selecting all key columns">
Select, like most other operators, can not change the key or timestamp columns.
As a result, not selecting all the key columns is a sync error.
</Expandable>

<Expandable title="Selecting non-existent column">
Sync error to select a column that is not present in the input dataset.
</Expandable>

</LeftSection>


<RightSection>
<pre snippet="api-reference/operators/select#basic" status="success"
    message="Selecting uid, height & weight columns">
</pre>
<pre snippet="api-reference/operators/select#missing_key" status="error"
    message="Did not select key uid">
</pre>
<pre snippet="api-reference/operators/select#missing_column" status="error"
    message="Selecting non-existent column">
</pre>
</RightSection>

</Divider>