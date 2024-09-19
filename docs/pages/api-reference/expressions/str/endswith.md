---
title: Ends With
order: 0
status: published
---

### Ends With

Function to check if the given string ends with the given another string.

#### Parameters
<Expandable title="item" type="str">
`endswith` checks if the input string ends with the expression `item`.
</Expandable>

<pre snippet="api-reference/expressions/str#endswith"
    status="success" message="Checking string suffix match with endswith">
</pre>


#### Returns
<Expandable type="Expr">
Returns an expression object denoting the result of the `endswith` expression.
The resulting expression is of type `bool` or `Optional[bool]` depending on
either of input/item being nullable.
</Expandable>


#### Errors
<Expandable title="Use of invalid types">
The `str` namespace must be invoked on an expression that evaluates to string
or optional of string. Similarly, `item` must evaluate to either a string or an
optional of string.
</Expandable>
