---
title: Summarize
order: 0
status: published
---
### Summarize

<Divider>
<LeftSection>
Operator to do summary over a window aggregation. Summarize operator must always be preceded by a [window](/api-reference/operators#window) operator.

#### Parameters


<Expandable title="field" type="str">
The name of the new column to be added to store the summary - must not conflict with any existing name on the dataset.
</Expandable>

<Expandable title="dtype" type="Type">
The data type of the new column to be added - must be a valid Fennel supported
[data type](/api-reference#api-reference/data-types).
</Expandable>

<Expandable title="func" type="Callable[pd.Dataframe, pd.Series[T]]">
The function, which when given a subset of the dataframe corresponding to the rows in the aggregation returns the summary value for that aggregation. 

Fennel verifies at runtime that the returned series matches the declared `dtype`.
</Expandable>

#### Returns
<Expandable type="Dataset">

Returns a dataset where all columns passed to groupby become the key columns, the timestamp column become the end timestamps of the window corresponding to that aggregation. One value column will be create to store the result of summarize.

The type of the summary column depends on the output of summary function.
</Expandable>

#### Errors
<Expandable title="Invalid value at runtime">
Runtime error if the value returned from the lambda isn't a value of
the declared type.
</Expandable>

</LeftSection>

<RightSection>
<pre snippet="api-reference/operators/summarize#basic" status="success"
    message="Aggregate event into sessions that are 15 minutes apart and calculate total amount"
>
</pre>

<pre snippet="api-reference/operators/summarize#wrong_type" status="error"
    message="The summarize result is defined as int but assign to type float"
>
</pre>

<pre snippet="api-reference/operators/summarize#runtime_error" status="error"
    message="The summarize result is a float but got a str in the schema type"
>
</pre>

</RightSection>
</Divider>

