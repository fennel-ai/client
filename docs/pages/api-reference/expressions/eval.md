---
title: Eval
order: 0
status: published
---
### Eval

Helper function to evaluate the value of an expression in the context of
a schema and a dataframe.

#### Parameters
<Expandable title="input_df" type="pd.Dataframe">
The dataframe for which the expression is evaluated - one value is produced
for each row in the dataframe.
</Expandable>

<Expandable title="schema" type="Dict[str, Type]">
The schema of the context under which the expression is to be evaluated. In the
case of pipelines, this will be the schema of the input dataset and in the case
of extractors, this will be the schema of the featureset.
</Expandable>

<pre snippet="api-reference/expressions/eval#expr_eval" 
    status="success" message="Using eval on a dataframe">
</pre>

#### Returns
<Expandable type="pd.Series">
Returns a series object of the same length as the number of rows in the input
dataframe.
</Expandable>


#### Errors
<Expandable title="Referenced column not provided">
All columns referenced by `col` expression must be present in both the 
dataframe and the schema.
</Expandable>

<Expandable title="Invalid expression">
The expression should be valid in the context of the given schema.
</Expandable>

<Expandable title="Runtime error">
Some expressions may produce a runtime error e.g. trying to parse an integer
out of a string may throw an error if the string doesn't represent an integer.
</Expandable>
