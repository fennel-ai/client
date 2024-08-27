---
title: Fill Null
order: 0
status: published
---
### Fill Null

The expression that is analogous to `fillna` in Pandas. 

#### Parameters

<Expandable title="expr" type="Expr">
The expression that will be checked for nullness.
</Expandable>

<Expandable title="fill" type="Expr">
The expression that will be substituted in case `expr` turns out to be null.
</Expandable>

<pre snippet="api-reference/expressions/basic#expr_fillnull"
    status="success" message="Using fillnull expression">
</pre>

#### Returns

<Expandable type="Expr">
Returns an expression object denoting the output of `fillnull` expression. 
If the `expr` is of type `Optional[T1]` and the `fill` is of type `T2`, the
type of the output expression is the smallest type that both `T1` and `T2` can
be promoted into. 

If the `expr` is not optional but is of type T, the output is trivially same as
`expr` and hence is also of type `T`.
</Expandable>