---
title: Core Types
order: 3
status: 'published'
---


### Core Types
<Divider>
<LeftSection>
Fennel supports the following data types, expressed as native Python type hints.
<Expandable type="int">
Implemented as signed 8 byte integer (`int64`)
</Expandable>

<Expandable type="float">
Implemented as signed 8 byte float with `double` precision
</Expandable>

<Expandable type="bool">
Implemented as standard 1 byte boolean
</Expandable>

<Expandable type="str">
Arbitrary sequence of utf-8 characters. Like most programming languages, `str` 
doesn't support arbitrary binary bytes though.
</Expandable>

<Expandable type="List[T]">
List of elements of any other valid type `T`. Unlike Python lists, all elements 
must have the same type.
</Expandable>

<Expandable type="dict[T]">
Map from `str` to data of any valid type `T`. 

Fennel does not support dictionaries with arbitrary types for keys - please 
reach out to Fennel support if you have use cases requiring that.
</Expandable>

<Expandable type="Optional[T]">
Same as Python `Optional` - permits either `None` or values of type `T`. 
</Expandable>

<Expandable type="Embedding[int]">
Denotes a list of floats of the given fixed length i.e. `Embedding[32]` 
describes a list of 32 floats. This is same as `list[float]` but enforces the 
list length which is important for dot product and other similar operations on 
embeddings.
</Expandable>

<Expandable type="datetime">
Describes a timestamp, implemented as microseconds since Unix epoch (so minimum 
granularity is microseconds). Can be natively parsed from multiple formats though
internally is stored as 8-byte signed integer describing timestamp as microseconds
from epoch in UTC.
</Expandable>

<Expandable type="struct {k1: T1, k2: T2, ...}">
Describes the equivalent of a struct or dataclass - a container containing a 
fixed set of fields of fixed types.
</Expandable>

:::info
Fennel uses a strong type system and post data-ingestion, data doesn't auto-coerce
across types. For instance, it will be a compile or runtime error if something 
was expected to be of type `float` but received an `int` instead.
:::

</LeftSection>
<RightSection>
    <pre snippet="api-reference/data-types#struct_type" />
</RightSection>
</Divider>

