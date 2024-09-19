---
title: Strptime
order: 0
status: published
---

### Strptime

Function to parse a datetime of the given format out of the string.

#### Parameters
<Expandable title="format" type="str">
A valid datetime format string. See 
[here](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) for a 
full list of all format qualifiers supported by Fennel.
</Expandable>

<Expandable title="timezone" type="Optional[str]" defaultVal="UTC">
Sometimes format strings don't precisely specify the timezone. In such cases, 
a timezone can be provided. In absence of an explicit timezone, all ambiguous 
strings are assumed to be in UTC.

Note that `timezone` is merely a hint to resolve disambiguity - the timezone
info from the format string is preferentially used when available.
</Expandable>


<pre snippet="api-reference/expressions/str#strptime"
    status="success" message="Parsing datetime objects out of string">
</pre>

#### Returns
<Expandable type="Expr">
Returns an expression object denoting the result of the `strptime` expression.
The resulting expression is of type `datetime` or `Optional[datetime]` depending on
either of input/item being nullable.
</Expandable>


#### Errors
<Expandable title="Use of invalid types">
The `str` namespace must be invoked on an expression that evaluates to string
or optional of string. 
</Expandable>

<Expandable title="Invalid format string or timezone">
Compile time error is raised if either of the format string or timezone is invalid.
</Expandable>