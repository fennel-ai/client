---
title: Log
order: 0
status: published
---

### Log

Function in `num` namespace to get the logarithm of a number.

#### Parameters
<Expandable title="base" type="float" defaultVal="2.718281828459045">
The base of the logarithm. By default, the base is set to `e` (Euler's number).
</Expandable>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the logarithm of the input data. The
data type of the resulting expression is `float` if the input was `int` or 
`float` and `Optional[float]` if the input was `Optional[int]` or 
`Optional[float]`.

For negative numbers, the result is `NaN` (Not a Number).
</Expandable>

<pre snippet="api-reference/expressions/num#log"
    status="success" message="Computing logarithm of a number">
</pre>

#### Errors
<Expandable title="Invoking on a non-numeric type">
Error during `typeof` or `eval` if the input expression is not of type int, 
float, optional int or optional float.
</Expandable>