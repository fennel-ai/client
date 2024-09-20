---
title: Sum
order: 0
status: published
---

### Sum

Function to get the sum of values of a list.

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the sum of the values of the list.

Only works when the list is of type int/float (or their optional versions). For
an empty list, returns an expression denoting '0'. If the list has one or more
`None` elements, the whole sum becomes `None`.
</Expandable>

<pre snippet="api-reference/expressions/list#sum"
    status="success" message="Summing the values of a list">
</pre>


#### Errors
<Expandable title="Use of invalid types">
The `list` namespace must be invoked on an expression that evaluates to list
or optional of list. `Sum` can only be invoked on lists of ints/floats (or 
optionals of ints/floats).
</Expandable>