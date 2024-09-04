---
title: Len
order: 0
status: published
---

### Len

Function to get the length of a list.

<pre snippet="api-reference/expressions/list#len"
    status="success" message="Getting the length of a list">
</pre>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the result of the `len` function.
The resulting expression is of type `int` or `Optional[int]` depending on
the input being nullable.
</Expandable>


#### Errors
<Expandable title="Use of invalid types">
The `list` namespace must be invoked on an expression that evaluates to list
or optional of list. 
</Expandable>
