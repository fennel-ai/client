---
title: Get
order: 0
status: published
---

### Get

Function to get a given field from a struct.

#### Parameters
<Expandable title="field" type="str">
The name of the field that needs to be obtained from the struct. Note that this
must be a literal string, not an expression.
</Expandable>

<pre snippet="api-reference/expressions/struct_snip#get"
    status="success" message="Get a field from a sturct">
</pre>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the result of the `get` operation.
If the corresponding field in the struct is of type `T`, the resulting expression
is of type `T` or `Optional[T]` depending on the struct itself being nullable.
</Expandable>


#### Errors
<Expandable title="Use of invalid types">
The `struct` namespace must be invoked on an expression that evaluates to struct.
</Expandable>

<Expandable title="Invalid field name">
Compile error is raised when trying to get a field that doesn't exist on the
struct.
</Expandable>