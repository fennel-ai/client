---
title: Summarize
order: 0
status: published
---
### Summarize
Operator to compute arbitrary aggregation summary over events in a window and 
augment that as a new field in the dataset.

Summarize operator must always be preceded by a [window](/api-reference/operators/window) operator.

#### Parameters
<Expandable title="field" type="str">
The name of the new column to be added to store the summary - must not conflict 
with any existing name on the dataset.
</Expandable>

<Expandable title="dtype" type="Type">
The data type of the new column to be added - must be a valid Fennel supported
[data type](/api-reference/data-types).
</Expandable>

<Expandable title="func" type="Callable[pd.Dataframe, pd.Series[T]]">
The function, which when given a subset of the dataframe corresponding to the 
rows falling in the window, returns the summary value for that window. No 
guarantees are made about the ordering of rows in this input dataframe.

Fennel verifies at runtime that the returned series matches the declared `dtype`.
</Expandable>

<pre snippet="api-reference/operators/summarize#basic" status="success"
    message="Calculate total amount per window in 15-min session windows">
</pre>

#### Returns
<Expandable type="Dataset">
Returns a dataset where all columns passed to groupby become the key columns, 
the timestamp column become the end timestamps of the window corresponding to 
that aggregation. One value column will be create to store the result of summarize. 
The type of the summary column depends on the output of summary function.
</Expandable>

#### Errors
<Expandable title="Invalid value at runtime">
Runtime error if the value returned from the lambda isn't a value of
the declared type.
</Expandable>

<pre snippet="api-reference/operators/summarize#wrong_type" status="error"
    message="The summarize result is defined as int but assign to type float">
</pre>

<pre snippet="api-reference/operators/summarize#runtime_error" status="error"
    message="The summarize result is an int but got a str in the schema type">
</pre>

