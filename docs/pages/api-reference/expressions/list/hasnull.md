---
title: Has Null
order: 0
status: published
---

### Has Null

Function to check if the given list has any `None` values.

<pre snippet="api-reference/expressions/list#has_null"
    status="success" message="Checking if a list has any null values">
</pre>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the result of the `hasnull` function.
The resulting expression is of type `bool` or `Optional[bool]` depending on
the input being nullable.
</Expandable>


#### Errors
<Expandable title="Use of invalid types">
The `list` namespace must be invoked on an expression that evaluates to list
or optional of list. 
</Expandable>
