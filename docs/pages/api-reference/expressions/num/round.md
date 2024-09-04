---
title: Round
order: 0
status: published
---

### Round

Function in `num` namespace to round a number.

#### Parameters
<Expandable title="precision" type="int" defaultVal="0">
The number of the decimal places to round the input to.
</Expandable>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the rounded value of the input data. The
data type of the resulting expression is `int` / `Optional[int]` if precision is 
set to `0` or `float` / `Optional[int]` for precisions > 0.
</Expandable>

<pre snippet="api-reference/expressions/num#ceil"
status="success" message="Rounding a number using Fennel expressions">
</pre>

#### Errors
<Expandable title="Invoking on a non-numeric type">
Error during `typeof` or `eval` if the input expression is not of type int, 
float, optional int or optional float.
</Expandable>

<Expandable title="Invalid precision">
Precision must be a non-negative integer.
</Expandable>