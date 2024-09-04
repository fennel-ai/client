---
title: Contains
order: 0
status: published
---

### Contains

Function to check if the given string contains another string.

#### Parameters
<Expandable title="item" type="str">
`contains` check if the base string contains `item` or not.
</Expandable>

<pre snippet="api-reference/expressions/str#contains"
    status="success" message="Checking if a string contains another string">
</pre>


#### Returns
<Expandable type="Expr">
Returns an expression object denoting the result of the `contains` expression.
The resulting expression is of type `bool` or `Optional[bool]` depending on
either of input/item being nullable.
</Expandable>


#### Errors
<Expandable title="Use of invalid types">
The `str` namespace must be invoked on an expression that evaluates to string
or optional of string. Similarly, `item` must evaluate to either a string or an
optional of string.
</Expandable>