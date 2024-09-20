---
title: From Epoch
order: 0
status: published
---

### From Epoch

Function to get a datetime object from a unix timestamp.

#### Parameters
<Expandable title="duration" type="Expr">
The duration (in units as specified by `unit`) since epoch to convert to a datetime
in the form of an expression denoting an integer.
</Expandable>

<Expandable title="unit" type="str" defaultVal="second">
The unit of the `duration` parameter. Can be one of `second`, `millisecond`, 
or `microsecond`. Defaults to `second`.
</Expandable>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the datetime object.
</Expandable>

<pre snippet="api-reference/expressions/dt#from_epoch"
    status="success" message="Getting a datetime from a unix timestamp">
</pre>