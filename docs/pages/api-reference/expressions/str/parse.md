---
title: Parse
order: 0
status: published
---

### Parse

Function to parse an object of the given type out of a string that represents json
encoded data.

#### Parameters
<Expandable title="dtype" type="Type">
The type of the data should be parsed from the json encoded string.
</Expandable>

<pre snippet="api-reference/expressions/str#parse_basic"
    status="success" message="Parsing a string into various types">
</pre>

<pre snippet="api-reference/expressions/str#parse_invalid"
    status="error" message="Common runtime errors during parsing">
</pre>

<pre snippet="api-reference/expressions/str#parse_struct"
    status="success" message="Can parse complex nested types too">
</pre>


#### Returns
<Expandable type="Expr">
Returns an expression object denoting the result of the `parse` expression.
The resulting expression is of type `dtype` or `Optional[dtype]` depending on
the base string being nullable.
</Expandable>

:::info
A type can only be parsed out of valid json representation of that type. For 
instance, a `str` can not be parsed out of `"hi"` because the correct json
representation of the string is `"\"hi\""`.
:::


#### Errors
<Expandable title="Use of invalid types">
The `str` namespace must be invoked on an expression that evaluates to string
or optional of string. 
</Expandable>


<Expandable title="Runtime parse error">
If the given string can not be parsed into an object of the given type, a runtime
error is raised.
</Expandable>