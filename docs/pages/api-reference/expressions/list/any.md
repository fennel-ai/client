---
title: Any
order: 0
status: published
---

### Any

Function to check if a boolean list contains any `True` value.

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the result of `any` operation.

Only works when the list is of type bool(or optional bool). For
an empty list, returns an expression denoting 'False'. If the list has one or more
`None` elements, the result becomes `None` unless it also has `True` in which case
the result is still `True`.

</Expandable>
<pre snippet="api-reference/expressions/list#any"
    status="success" message="Checking if the list has any True value">
</pre>


#### Errors
<Expandable title="Use of invalid types">
The `list` namespace must be invoked on an expression that evaluates to list
or optional of list. `Any` can only be invoked on lists of bool (or 
optionals of bool).
</Expandable>
