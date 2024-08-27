---
title: String Startswith
order: 0
status: published
---

### str.startswith

Function in `str` namespace to check if the given string starts with another
string.

#### Parameters
<Expandable title="name" type="str">
The name of the column being referenced. In the case of pipelines, this will
typically be the name of the field and in the case of extractors, this will
be the name of the feature.
</Expandable>

<pre snippet="api-reference/expressions/basic#expr_col"
    status="success" message="Referencing columns of a dataframe using col">
</pre>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting a reference to the column. The type of
the resulting expression is same as that of the referenced column. When evaluated
in the context of a dataframe, the value of the expression is same as the 
value of the dataframe column of that name.
</Expandable>


#### Errors
<Expandable title="Referenced column not provided">
Error during `typeof` or `eval` if the referenced column isn't present.
</Expandable>
