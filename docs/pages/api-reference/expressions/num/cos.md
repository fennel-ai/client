---
title: Cos
order: 0
status: published
---

### Cos

Function in `num` namespace to get the value of the cosine function.

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the cos of the input data (expected
to be in radians).

The data type of the resulting expression is `float` if the input is `int` or 
`float` and `Optional[float]` if the input is `Optional[int]` or `Optional[float]`.
</Expandable>


<pre snippet="api-reference/expressions/num#cos"
    status="success" message="Getting cos of a number">
</pre>

#### Errors
<Expandable title="Invoking on a non-numeric type">
Error during `typeof` or `eval` if the input expression is not of type int, 
float, optional int or optional float.
</Expandable>
