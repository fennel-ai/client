---
title: Since Epoch
order: 0
status: published
---

### Since Epoch

Function to get the time elapsed since epoch for a datetime object.

#### Parameters
<Expandable title="unit" type="Optional[str]" defaultVal="second">
The unit of time to return the elapsed time in. Defaults to seconds. Valid units
are: `week`, `day`,`hour`, `minute`, `second`, `millisecond`, and `microsecond`.
</Expandable>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the integer value of the elapsed time
since epoch for the datetime object in the specified unit.
</Expandable>

<pre snippet="api-reference/expressions/dt#since_epoch"
    status="success" message="Getting the elapsed time since epoch for a datetime">
</pre>


#### Errors
<Expandable title="Use of invalid types">
The `dt` namespace must be invoked on an expression that evaluates to datetime
or optional of datetime.
</Expandable>
