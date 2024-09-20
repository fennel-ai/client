---
title: Since
order: 0
status: published
---

### Since

Function to get the time elapsed between two datetime objects.

#### Parameters
<Expandable title="other" type="Expr">
The datetime object to calculate the elapsed time since.
</Expandable>

<Expandable title="unit" type="Optional[str]" defaultVal="second">
The unit of time to return the elapsed time in. Defaults to seconds. Valid units
are: `week`, `day`,`hour`, `minute`, `second`, `millisecond`, and `microsecond`.
</Expandable>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the integer value of the elapsed time
since the specified datetime object in the specified unit.
</Expandable>

<pre snippet="api-reference/expressions/dt#since"
    status="success" message="Getting the elapsed time since a datetime">
</pre>


#### Errors
<Expandable title="Use of invalid types">
The `dt` namespace must be invoked on an expression that evaluates to datetime
or optional of datetime.
</Expandable>
