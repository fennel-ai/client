---
title: Starts With
order: 0
status: published
---

### Starts With

Function to check if the given string starts with another string.

#### Parameters
<Expandable title="item" type="str">
`startswith` checks if the input string starts with the expression `item`.
</Expandable>

<pre snippet="api-reference/expressions/str#startswith"
    status="success" message="Checking string prefix match with startswith">
</pre>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the result of the `startswith` expression.
The resulting expression is of type `bool` or `Optional[bool]` depending on
either of input/item being nullable.
</Expandable>


#### Errors
<Expandable title="Use of invalid types">
The `str` namespace must be invoked on an expression that evaluates to string
or optional of string. Similarly, `item` must evaluate to either a string or an
optional of string.
</Expandable>
