---
title: Strftime
order: 0
status: published
---

### Strftime

Function to format a datetime object as a string.

#### Parameters
<Expandable title="format" type="str">
The format string to use for the datetime.
</Expandable>

<Expandable title="timezone" type="Optional[str]" defaultVal="UTC">
The timezone in which to interpret the datetime. If not specified, UTC is used.
</Expandable>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the formatted datetime string.
</Expandable>

<pre snippet="api-reference/expressions/dt#strftime"
    status="success" message="Formatting a datetime">
</pre>


#### Errors
<Expandable title="Use of invalid types">
The `dt` namespace must be invoked on an expression that evaluates to datetime
or optional of datetime.
</Expandable>

<Expandable title="Invalid format string">
The format string must be a valid format string.
</Expandable>

<Expandable title="Invalid timezone">
The timezone must be a valid timezone. Note that Fennel only supports timezones
with area/location names (e.g. `America/New_York`) and not timezones with offsets
(e.g. `+05:00`).
</Expandable>
