---
title: Abs
order: 0
status: published
---

### Abs

Function to get the absolute value of a number.

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the absolute value of the input data. The
data type of the resulting expression is same as that of the input. 
</Expandable>

<pre snippet="api-reference/expressions/num#abs"
status="success" message="Getting absolute value of numeric using abs">
</pre>

#### Errors
<Expandable title="Invoking on a non-numeric type">
Error during `typeof` or `eval` if the input expression is not of type int, 
float, optional int or optional float.
</Expandable>