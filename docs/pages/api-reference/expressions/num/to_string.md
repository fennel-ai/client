---
title: To String
order: 0
status: published
---

### To String

Function in `num` namespace to convert a number to a string.

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the string value of the input data. The
data type of the resulting expression is `str` (or `Optional[str]` if the input
is an optional number).
</Expandable>

<pre snippet="api-reference/expressions/num#to_string"
status="success" message="Converting a number to a string using Fennel expressions">
</pre>

#### Errors
<Expandable title="Invoking on a non-numeric type">
Error during `typeof` or `eval` if the input expression is not of type int, 
float, optional int or optional float.
</Expandable>