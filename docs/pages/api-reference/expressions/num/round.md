---
title: Round
order: 0
status: published
---

### round

Function in `num` namespace to round a number.

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the ceil of the input data. The
data type of the resulting expression is `int` if the input was `int` or `float` 
or `Optional[int]` when the input is `Optional[int]` or `Optional[float]`.
</Expandable>

<pre snippet="api-reference/expressions/num#ceil"
status="success" message="Getting ceil value of a number">
</pre>

#### Errors
<Expandable title="Invoking on a non-numeric type">
Error during `typeof` or `eval` if the input expression is not of type int, 
float, optional int or optional float.
</Expandable>