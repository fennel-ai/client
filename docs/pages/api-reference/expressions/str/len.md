---
title: Len
order: 0
status: published
---

### Len

Function to get the length of a string

<pre snippet="api-reference/expressions/str#len"
    status="success" message="Getting the length of a string">
</pre>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the result of the `len` function.
The resulting expression is of type `int` or `Optional[int]` depending on
input being nullable.
</Expandable>


#### Errors
<Expandable title="Use of invalid types">
The `str` namespace must be invoked on an expression that evaluates to string
or optional of string. 
</Expandable>
