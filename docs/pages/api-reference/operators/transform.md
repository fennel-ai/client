---
title: Transform
order: 0
status: published
---
### Transform
Catch all operator to add/remove/update columns.

#### Parameters

<Expandable title="func" type="Callable[pd.Dataframe, pd.Dataframe]">
The transform function that takes a pandas dataframe containing a batch of rows 
from the input dataset and returns an output dataframe of the same length, 
though potentially with different set of columns.
</Expandable>

<Expandable title="schema" type="Optional[Dict[str, Type]]" default="None">
The expected schema of the output dataset. If not specified, the schema of the
input dataset is used.
</Expandable>

<pre snippet="api-reference/operators/transform#basic" status="success"
    message="Adding column amount_sq" highlight="12, 21">
</pre>

#### Returns

<Expandable type="Dataset">
Returns a dataset with the schema as specified in `schema` and rows as transformed
by the transform function.
</Expandable>

#### Errors

<Expandable title="Output dataframe doesn't match the schema">
Runtime error if the dataframe returned by the transform function doesn't match
the provided `schema`.
</Expandable>

<Expandable title="Modifying key/timestamp columns">
Sync error if transform tries to modify key/timestamp columns.
</Expandable>

<pre snippet="api-reference/operators/transform#modifying_keys" status="error"
    message="Modifying key or timestamp columns" highlight="4, 9, 10, 23">
</pre>

<pre snippet="api-reference/operators/transform#incorrect_type" status="error"
    message="Runtime error: amount_sq is of type int, not str" highlight="12, 21">
</pre>
