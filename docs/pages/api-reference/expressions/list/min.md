---
title: Min
order: 0
status: published
---

### Min

Function to get the min value of a list.

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the min value of a list.

Only works when the list is of type int/float (or their optional versions). For
an empty list, returns an expression denoting 'None'. If the list has one or more
`None` elements, the result becomes `None`.

</Expandable>
<pre snippet="api-reference/expressions/list#min"
    status="success" message="Taking the minimum value of a list">
</pre>


#### Errors
<Expandable title="Use of invalid types">
The `list` namespace must be invoked on an expression that evaluates to list
or optional of list. `Min` can only be invoked on lists of ints/floats (or 
optionals of ints/floats).
</Expandable>