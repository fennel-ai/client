---
title: At
order: 0
status: published
---

### At

Function to get the value of the element at a given index of the list.

#### Parameters
<Expandable title="index" type="Expr">
The index at which list's value needs to be evaluated. This expression is expected
to evaluate to an int. Fennel supports indexing by negative integers as well.
</Expandable>

<pre snippet="api-reference/expressions/list#at"
    status="success" message="Getting the value of a list's element at given index">
</pre>

<pre snippet="api-reference/expressions/list#at_negative"
    status="success" message="Also works with negative indices">
</pre>


#### Returns
<Expandable type="Expr">
Returns an expression object denoting the value of the list at the given index.
If the index is out of bounds of list's length, `None` is returned. Consequently,
for a list of elements of type `T`, `at` always returns `Optional[T]`.

Fennel also supports negative indices: -1 maps to the last element of the list, 
-2 to the second last element of the list and so on. Negative indices smaller 
than -len start returning `None` like other out-of-bound indices.
</Expandable>


#### Errors
<Expandable title="Use of invalid types">
The `list` namespace must be invoked on an expression that evaluates to list
or optional of list. Similarly, `index` must evaluate to an element of type `int`
or `Optional[int]`.
</Expandable>