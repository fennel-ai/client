---
title: Json Extract
order: 0
status: published
---

### Json Extract

Function to extract a value from a json encoded string using a json path.

#### Parameters
<Expandable title="path" type="str">
The json path to use when extracting the value from the json encoded string.
See [this page](https://goessner.net/articles/JsonPath/) for more details on
json path syntax. The extracted value is always returned as a string or None
if the path is not valid/found.
</Expandable>

<pre snippet="api-reference/expressions/str#json_extract"
    status="success" message="Extracting a value from a json encoded string">
</pre>


#### Returns
<Expandable type="Expr">
Returns an expression object denoting the result of the `json_extract` expression.
The resulting expression is of type `Optional[str]` and more specifically is None
when the base string is None or the path is not found in the json encoded string.
</Expandable>


#### Errors
<Expandable title="Use of invalid types">
The `str` namespace must be invoked on an expression that evaluates to string
or optional of string. 
</Expandable>