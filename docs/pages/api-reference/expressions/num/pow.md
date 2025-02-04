---
title: Pow
order: 0
status: published
---

### Pow

Function in `num` namespace to exponentiate a number.

#### Parameters
<Expandable title="exponent" type="Expr">
The exponent to which the base is raised - expected to be a numeric expression.
</Expandable>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the result of the exponentiation.

The base data type of the resulting expression is `int` if both the base and 
exponent are `int`, otherwise it is `float`.

If any of the base or exponent is `Optional`, the resulting expression is 
also `Optional` of the base data type.
</Expandable>

<pre snippet="api-reference/expressions/num#pow" status="success" 
    message="Exponentiating a number">
</pre>

#### Errors

<Expandable title="Invoking on a non-numeric type">
Error during `typeof` or `eval` if the input expression is not of type int, 
float, optional int or optional float.
</Expandable>


<Expandable title="Exponentiation of negative integers">
A runtime error will be raised if the exponent is a negative integer and the 
base is also an integer.

In such cases, it's advised to convert either the base or the exponent to be a 
float.
</Expandable>
