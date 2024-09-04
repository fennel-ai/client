---
title: Upper
order: 0
status: published
---

### Upper

Function to convert a string to all upper case letters.

<pre snippet="api-reference/expressions/str#upper"
    status="success" message="Making a string upper case">
</pre>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the result of the `upper` function.
The resulting expression is of type `str` or `Optional[str]` depending on
input being nullable.
</Expandable>


#### Errors
<Expandable title="Use of invalid types">
The `str` namespace must be invoked on an expression that evaluates to string
or optional of string. 
</Expandable>
