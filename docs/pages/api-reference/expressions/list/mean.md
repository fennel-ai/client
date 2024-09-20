---
title: Mean
order: 0
status: published
---

### Mean

Function to get the mean of the values of a list.

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the mean value of a list.

Only works when the list is of type int/float (or their optional versions). For
an empty list, returns an expression denoting 'None'. If the list has one or more
`None` elements, the result becomes `None`.

The output type of this expression is either `float` or `Optional[float]` depending
on the inputs.

</Expandable>
<pre snippet="api-reference/expressions/list#mean"
    status="success" message="Taking the average value of a list">
</pre>


#### Errors
<Expandable title="Use of invalid types">
The `list` namespace must be invoked on an expression that evaluates to list
or optional of list. `Mean` can only be invoked on lists of ints/floats (or 
optionals of ints/floats).
</Expandable>
