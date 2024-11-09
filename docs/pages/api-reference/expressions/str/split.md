---
title: Split
order: 0
status: published
---

### Split

Function to split a string into a list of strings using a separator.

#### Parameters
<Expandable title="sep" type="str">
The separator string to use when splitting the string.
</Expandable>

<pre snippet="api-reference/expressions/str#split"
    status="success" message="Splitting a string by comma">
</pre>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the result of the `split` function.
The resulting expression is of type `List[str]` or `Optional[List[str]]` depending on
input being nullable.
</Expandable>


#### Errors
<Expandable title="Use of invalid types">
The `str` namespace must be invoked on an expression that evaluates to string
or optional of string. 
</Expandable>
