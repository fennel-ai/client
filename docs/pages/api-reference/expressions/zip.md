---
title: Zip
order: 0
status: published
---
### Zip

Zip two or more lists into a list of structs.

#### Parameters
<Expandable title="struct" type="Struct">
The struct to hold the zipped values. Unlike other top level expressions, 
`zip` is written as `Struct.zip(kwarg1=expr1, kwarg2=expr2, ...)`.
</Expandable>

<Expandable title="kwargs" type="Dict[str, Expr]">
A dictionary of key-value pairs where the key is the name of the field in the 
struct and the value is the expression to zip.

Expressions are expected to evaluate to lists of a type that can be converted to
the corresponding field type in the struct.
</Expandable>


<pre snippet="api-reference/expressions/basic#zip" status="success" 
    message="Zipping two lists into a list of structs">
</pre>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the result of the zip expression.
</Expandable>

:::info
When zipping lists of unequal length, similar to Python's zip function, the 
resulting list will be truncated to the length of the shortest list, possibly
zero.
:::

#### Errors
<Expandable title="Mismatching types">
An error is thrown if the types of the lists to zip are not compatible with the
field types in the struct.
</Expandable>

<Expandable title="Non-list types">
An error is thrown if the expressions to zip don't evaluate to lists.
</Expandable>
