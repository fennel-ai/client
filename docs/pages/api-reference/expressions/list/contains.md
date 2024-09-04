---
title: Contains
order: 0
status: published
---

### Contains

Function to check if the given list contains a given element.

#### Parameters
<Expandable title="item" type="Expr">
`contains` check if the base list contains the `item` or not.
</Expandable>

<pre snippet="api-reference/expressions/list#contains"
    status="success" message="Checking if a list contains a given item">
</pre>


#### Returns
<Expandable type="Expr">
Returns an expression object denoting the result of the `contains` expression.
The resulting expression is of type `bool` or `Optional[bool]` depending on
either of input/item being nullable.

Note that, Fennel expressions borrow semantics from SQL and treat `None` as 
an unknown value. As a result, the following rules apply to `contains` in 
presence of nulls:
- If the base list itself is `None`, the result is `None` regardless of the item.
- If the item is `None`, the result is `None` regardless of the list, unless it
  is empty, in which case, the answer is `False` (after all, if the list is empty,
  no matter the value of the item, it's not present in the list).
- If the item is not `None` and is present in the list, the answer is obviously
  `True`
- However, if the item is not `None`, is not present in the list but the list
  has some `None` element, the result is still `None` (because the `None` values
  in the list may have been that element - we just can't say)

This is somewhat (but not exactly) similar to Spark's `array_contains` [function](https://docs.databricks.com/en/sql/language-manual/functions/array_contains.html).
</Expandable>
:::info
If you are interested in checking if a list has any `None` elements, a better
way of doing that is to use [hasnull](/api-reference/expressions/list/hasnull).
:::


#### Errors
<Expandable title="Use of invalid types">
The `list` namespace must be invoked on an expression that evaluates to list
or optional of list. Similarly, `item` must evaluate to an element of type `T`
or `Optional[T]` if the list itself was of type `List[T]` (or `Optional[List[T]]`)
</Expandable>