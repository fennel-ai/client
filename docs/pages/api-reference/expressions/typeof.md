---
title: Typeof
order: 0
status: published
---
### Typeof

Helper function to figure out the inferred type of any expression.

#### Parameters
<Expandable title="schema" type="Dict[str, Type]">
The schema of the context under which the expression is to be analyzed. In the
case of pipelines, this will be the schema of the input dataset and in the case
of extractors, this will be the schema of the featureset.
</Expandable>

<pre snippet="api-reference/expressions/eval#expr_typeof" 
    status="success" message="Using typeof to check validity and type of 
expressions">
</pre>

#### Returns
<Expandable type="Type">
Returns the inferred type of the expression, if any.
</Expandable>


#### Errors
<Expandable title="Type of a referenced column not provided">
All columns referenced by `col` expression must be present in the provided
schema.
</Expandable>

<Expandable title="Invalid expression">
The expression should be valid in the context of the given schema.
</Expandable>
