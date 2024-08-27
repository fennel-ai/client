---
title: Binary Operations
order: 0
status: published
---
### Binary Operations
Standard binary operations - arithmetic operations (`+`, `-`, `*`, `/`, `%`), 
relational operations (`==`, `!=`, `>`, `>=`, `<`, `>=`) and logical operations 
(`&`, `|`). Note that logical `and` is represented as `&` and logical `or` is 
represented as `|`. Bitwise operations aren`t yet supported.

#### Typing Rules

1. All arithmetic operations and all comparison operations (i.e. `>`, `>=`, `<`
   `<=`) are only permitted on numerical data - ints/floats or options of 
   ints/floats. In such cases, if even one of the operands is of type float, the
   whole expression is promoted to be of type of float.
2. Logical operations `&`, `|` are only permitted on boolean data.
3. If even one of the operands is optional, the whole expression is promoted to
   optional type.
4. None, like many SQL dialects, is interpreted as some unknown value. As a 
   result, `x` + None is None for all `x`. None values are 'viral' in the sense
   that they usually make value of the whole expression None. Some notable 
   exceptions : `False & None` is still False and `True | None` is still True
   irrespective of the unknown value that None represents.

<pre snippet="api-reference/expressions/binary#expr_binary_arithmetic"
    status="success" message="Using binary expressions">
</pre>
