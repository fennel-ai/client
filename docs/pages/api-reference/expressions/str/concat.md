---
title: Concat
order: 0
status: published
---

### Concat

Function to concatenate two strings.

#### Parameters
<Expandable title="item" type="str">
The string to be concatenated with the base string.
</Expandable>

<pre snippet="api-reference/expressions/str#concat"
    status="success" message="Concatinating two strings">
</pre>


#### Returns
<Expandable type="Expr">
Returns an expression object denoting the result of the `concact` expression.
The resulting expression is of type `str` or `Optional[str]` depending on
either of input/item being nullable.
</Expandable>


#### Errors
<Expandable title="Use of invalid types">
The `str` namespace must be invoked on an expression that evaluates to string
or optional of string. Similarly, `item` must evaluate to either a string or an
optional of string.
</Expandable>
