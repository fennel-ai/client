---
title: Filter
order: 0
status: published
---

### Filter

Function to filter a list down to elements satisfying a predicate.

#### Parameters
<Expandable title="var" type="str">
The variable name to which each element of the list should be bound to
one-by-one. 
</Expandable>

<Expandable title="predicate" type="Expr">
The predicate expression to be used to filter the list down. This must
evaluate to bool for each element of the list. Note that this expression can
refer to the element under consideration via `var(name)` where name is the 
first argument given to the `filter` operation (see example for details).
</Expandable>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the filtered list.
</Expandable>

<pre snippet="api-reference/expressions/list#filter"
    status="success" message="Filtering the list to only even numbers">
</pre>


#### Errors
<Expandable title="Use of invalid types">
The `list` namespace must be invoked on an expression that evaluates to list
or optional of list. 
</Expandable>