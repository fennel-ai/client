---
title: Strong Typing
order: 1
status: 'published'
---

# Strong Typing

Fennel supports a rich and powerful [data type system](/api-reference/data-types/core-types). 
All dataset fields and features in Fennel must be given a type and Fennel 
enforces these types strongly. In particular, types don't auto typecast (e.g.
`int` values can not be passed where `float` is expected) and nullable types
are explicitly declared (e.g. `Optional[str]` can take nulls but not `str`).

Let's see how this helps prevent quality bugs:

## Dataset Fields

Every dataset field must be given a type. Fennel simply rejects any incoming data
that doesn't match the type of any field in the dataset - as a result, datasets
can always be trusted to only have type compliant data. This prevents any downstream
bugs/failures arising due to operations on invalid data.


## Type Restrictions

Sometimes application data models require much finer grained enforcement of types
than what is supported by programming languages. For instance, if a dataset field
represents a zip code, while the datatype is `str`, only a subset of strings that
match a zip code regex are semantically valid. 

Or as another example, if a dataset field represents gender, maybe only a 
handful of values are valid (e.g. `male`, `female`, `non-binary` etc.). Fennel's 
type system supports [type restrictions](/api-reference/data-types/type-restrictions)
using which all these and lot more constraints can be encoded as data types and 
thus get checked at compile and runtime everywhere.


## Datetime Parsing

Timestamps can be encoded in a variety of formats and this often creates a bunch of
bugs in data engineering world. Fennel has a separate data type for `datetime` which
is automatically parsed from a wide variety of formats. As a result, some of the data
may be encoding time as milliseconds since epoch and another as a string in RFC 3339
format and Fennel supports their inter-operation quite nicely.