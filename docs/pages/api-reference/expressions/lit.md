---
title: Lit
order: 0
status: published
---
### Lit

Fennel's way of describing constants, similar to `lit` in Polars or Spark.

#### Parameters
<Expandable title="const" type="Any">
The literal/constant Python object that is to be used as an expression in Fennel.
This can be used to construct literals of ints, floats, strings, boolean, lists,
structs etc. Notably though, it's not possible to use `lit` to build datetime
literals.
</Expandable>

<pre snippet="api-reference/expressions/basic#expr_lit"
    message="Using lit to describe contants" status="success">
</pre>

#### Returns
<Expandable type="Any">
The expression that denotes the literal value.
</Expandable>
