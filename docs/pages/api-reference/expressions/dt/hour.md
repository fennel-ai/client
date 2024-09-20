---
title: Hour
order: 0
status: published
---

### Hour

Function to get the hour component of a datetime object.

#### Parameters
<Expandable title="timezone" type="Optional[str]" defaultVal="UTC">
The timezone in which to interpret the datetime. If not specified, UTC is used.
</Expandable>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the integer value of the hour of the
datetime object.
</Expandable>

<pre snippet="api-reference/expressions/dt#hour"
    status="success" message="Getting the hour of a datetime">
</pre>


#### Errors
<Expandable title="Use of invalid types">
The `dt` namespace must be invoked on an expression that evaluates to datetime
or optional of datetime.
</Expandable>

<Expandable title="Invalid timezone">
The timezone, if provided, must be a valid timezone string. Note that Fennel
only supports area/location based timezones (e.g. "America/New_York"), not
fixed offsets (e.g. "+05:30" or "UTC+05:30").
</Expandable>