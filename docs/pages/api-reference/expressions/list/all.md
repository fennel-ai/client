---
title: All
order: 0
status: published
---

### All

Function to check if all the elements in a boolean list are `True`.

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the result of the `all` operation.

Only works when the list is of type bool or Optional[bool]. For an empty list, 
returns an expression denoting `True`. If the list has one or more `None` 
elements, the result becomes `None`.

</Expandable>
<pre snippet="api-reference/expressions/list#all"
    status="success" message="Checking if all elements of a list are True">
</pre>


#### Errors
<Expandable title="Use of invalid types">
The `list` namespace must be invoked on an expression that evaluates to list
or optional of list. `All` can only be invoked on lists of bools (or 
optionals of bool).
</Expandable>
