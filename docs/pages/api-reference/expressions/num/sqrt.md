---
title: Sqrt
order: 0
status: published
---

### Sqrt

Function in `num` namespace to get the square root of a number.

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the square root of the input data. 

The data type of the resulting expression is `float` if the input is `int` or 
`float` and `Optional[float]` if the input is `Optional[int]` or `Optional[float]`.
</Expandable>

:::info
The square root of a negative number is represented as `NaN` in the output.
:::

<pre snippet="api-reference/expressions/num#sqrt"
    status="success" message="Getting square root of a number">
</pre>

#### Errors
<Expandable title="Invoking on a non-numeric type">
Error during `typeof` or `eval` if the input expression is not of type int, 
float, optional int or optional float.
</Expandable>
