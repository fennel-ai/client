---
title: When
order: 0
status: published
---
### When

Ternary expressions like 'if/else' or 'case' in SQL.

#### Parameters
<Expandable title="when" type="Expr">
The predicate expression for the ternary operator. Must evaluate to a boolean.
</Expandable>


<Expandable title="then" type="Expr">
The expression that the whole when expression evaluates to if the predictate
evaluates to True. `then` must always be called on the result of a `when` 
expression.
</Expandable>

<Expandable title="otherwise" type="Expr">
The equivalent of `else` branch in the ternary expression - the whole expression
evaluates to this branch when the predicate evaluates to be False.
</Expandable>


<pre snippet="api-reference/expressions/basic#expr_when_then"
status="success" message="Conditionals using when expressions">
</pre>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the result of the when/then/otherwise
expression.
</Expandable>


#### Errors
<Expandable title="Referenced column not provided">
Error during `typeof` or `eval` if the referenced column isn't present.
</Expandable>

<Expandable title="Malformed expressions">
Valid `when` expressions must have accompanying `then` and `otherwise` 
clauses. 
</Expandable>
