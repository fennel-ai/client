---
title: Lower
order: 0
status: published
---

### Lower

Function to convert a string to all lowercase letters.

<pre snippet="api-reference/expressions/str#lower"
    status="success" message="Making a string lowercase">
</pre>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the result of the `lower` function.
The resulting expression is of type `str` or `Optional[str]` depending on
input being nullable.
</Expandable>


#### Errors
<Expandable title="Use of invalid types">
The `str` namespace must be invoked on an expression that evaluates to string
or optional of string. 
</Expandable>
