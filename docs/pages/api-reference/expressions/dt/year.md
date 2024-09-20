---
title: Year
order: 0
status: published
---

### Year

Function to get the year component of a datetime object.

#### Parameters
<Expandable title="timezone" type="Optional[str]" defaultVal="UTC">
The timezone in which to interpret the datetime. If not specified, UTC is used.
</Expandable>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the integer value of the year of the
datetime object.
</Expandable>

<pre snippet="api-reference/expressions/dt#year"
    status="success" message="Getting the year of a datetime">
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