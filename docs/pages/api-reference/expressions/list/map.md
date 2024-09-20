---
title: Map
order: 0
status: published
---

### Map

Function to map each element of a list to get another list of the same size.

#### Parameters
<Expandable title="var" type="str">
The variable name to which each element of the list should be bound to
one-by-one. 
</Expandable>

<Expandable title="expr" type="Expr">
The expression to be used to transform each element of the list. Note that 
this expression can refer to the element under consideration via `var(name)` 
where name is the first argument given to the `map` operation (see example for 
details).
</Expandable>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the transformed list.
</Expandable>

<pre snippet="api-reference/expressions/list#map"
    status="success" message="Transforming the list to get another list">
</pre>


#### Errors
<Expandable title="Use of invalid types">
The `list` namespace must be invoked on an expression that evaluates to list
or optional of list. 
</Expandable>