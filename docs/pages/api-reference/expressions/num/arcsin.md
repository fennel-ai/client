---
title: Arcsin
order: 0
status: published
---

### Arcsin

Function in `num` namespace to get the value of the inverse trigonometric
sine function.

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the arcsin of the input data (in radians).

The data type of the resulting expression is `float` if the input is `int` or 
`float` and `Optional[float]` if the input is `Optional[int]` or `Optional[float]`.
</Expandable>


:::info
Invoking `arcsin` outside the range [-1, 1] will return `NaN`.
:::


<pre snippet="api-reference/expressions/num#arcsin"
    status="success" message="Getting arcsin of a number">
</pre>

#### Errors
<Expandable title="Invoking on a non-numeric type">
Error during `typeof` or `eval` if the input expression is not of type int, 
float, optional int or optional float.
</Expandable>

