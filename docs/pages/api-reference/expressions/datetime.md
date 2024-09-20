---
title: Datetime
order: 0
status: published
---

### Datetime

Function to get a constant datetime object from its constituent parts.

#### Parameters
<Expandable title="year" type="int">
The year of the datetime. Note that this must be an integer, not an
expression denoting an integer.
</Expandable>

<Expandable title="month" type="int">
The month of the datetime. Note that this must be an integer, not an
expression denoting an integer.
</Expandable>

<Expandable title="day" type="int">
The day of the datetime. Note that this must be an integer, not an
expression denoting an integer.
</Expandable>

<Expandable title="hour" type="int" defaultVal="0">
The hour of the datetime. Note that this must be an integer, not an
expression denoting an integer.

</Expandable>

<Expandable title="minute" type="int" defaultVal="0">
The minute of the datetime. Note that this must be an integer, not an
expression denoting an integer.
</Expandable>

<Expandable title="second" type="int" defaultVal="0">
The second of the datetime. Note that this must be an integer, not an
expression denoting an integer.
</Expandable>

<Expandable title="millisecond" type="int" defaultVal="0">
The millisecond of the datetime. Note that this must be an integer, not an
expression denoting an integer.
</Expandable>

<Expandable title="microsecond" type="int" defaultVal="0">
The microsecond of the datetime. Note that this must be an integer, not an
expression denoting an integer.
</Expandable>

<Expandable title="timezone" type="Optional[str]" defaultVal="UTC">
The timezone of the datetime. Note that this must be a string denoting a valid
timezone, not an expression denoting a string.
</Expandable>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the datetime object.
</Expandable>

<pre snippet="api-reference/expressions/dt#datetime"
    status="success" message="Getting a datetime from its constituent parts">
</pre>


#### Errors
<Expandable title="Invalid datetime parts">
The month must be between 1 and 12, the day must be between 1 and 31, the hour
must be between 0 and 23, the minute must be between 0 and 59, the second must be
between 0 and 59, the millisecond must be between 0 and 999, and the
microsecond must be between 0 and 999.

Timezone, if provided, must be a valid timezone string. Note that Fennel only
supports area/location based timezones (e.g. "America/New_York"), not fixed
offsets (e.g. "+05:30" or "UTC+05:30").
</Expandable>