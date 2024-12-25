---
title: Repeat
order: 0
status: published
---
### Repeat

Repeat an expression `n` times to create a list.

#### Parameters
<Expandable title="value" type="Expr">
The expression to repeat.
</Expandable>

<Expandable title="by" type="Expr">
The number of times to repeat the value - can evaluate to a different count for 
each row.
</Expandable>


<pre snippet="api-reference/expressions/basic#repeat"
status="success" message="Repeating booleans to create list">
</pre>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the result of the repeat expression.
</Expandable>


#### Errors
<Expandable title="Invalid input types">
An error is thrown if the `by` expression is not of type int.
In addition, certain types (e.g. lists) are not supported as input for `value`.
</Expandable>

<Expandable title="Negative count">
An error is thrown if the `by` expression evaluates to a negative integer.
</Expandable>
