---
title: 'Dataset'
order: 2
status: 'published'
---

# Dataset

Datasets refer to a table like data with typed columns. Datasets can be 
[sourced](/concepts/source) from external datasets or derived from other
datasets via [pipelines](/concepts/pipeline). Datasets are written as 
[Pydantic](https://docs.pydantic.dev/) inspired Python classes decorated 
with the `@dataset` decorator.

### Example

<pre snippet="datasets/datasets#user_dataset" />

### Dataset Schema

A dataset has few fields (interchangeably referred to as columns) with types and
unique names. Each field must have has a pre-specified datatype. See the 
[typing](/api-reference/data-types.md) section to learn the types supported by
Fennel.

### Field Descriptors

You might have noticed the `field(...)` descriptor next to `uid` and 
`update_time` fields. These optional descriptors are used to provide non-typing
related information about the field. Currently, Fennel supports two
field descriptors:

**Key Fields**

Key fields are those with `field(key=True)` set on them. The semantics of this 
are somewhat similar to those of primary key in relational datasets. Datasets 
can be looked-up by providing the value of key fields. It is okay to have a 
dataset with zero key fields (e.g. click streams) - in those cases, it's not 
possible to do lookups on the dataset at all. Fennel also supports having 
multiple key fields on a dataset - in that case, all of those need to be 
provided while doing a lookup. Field with optional data types can not be key
fields. 

**Timestamp Field**

Timestamp fields are those with `field(timestamp=True)` set on them. Every 
dataset must have exactly one timestamp field and this field should always
be of type `datetime`. Semantically, this field must represent 'event time'
of the row. Fennel uses timestamp field to associate a particular state of
a row with a timestamp. This allows Fennel to handle out of order events, 
do time window aggregations, and compute point in time correct features for
training data generation.

If a dataset has exactly one field of `datetime` type, it is assumed to be
the timestamp field of the dataset without explicit annotation. However, if
a dataset has multiple timestamp fields, one of those needs to be explicitly
annotated to be the timestamp field.

Here are some examples of valid and invalid datasets:

Valid - has no key fields, which is fine. 
No explicitly marked timestamp fields so update_time, which is of type
datetime is automatically assumed to be the timestamp field

<pre snippet="datasets/datasets#valid_user_dataset" />


Invalid - key fields can not have an optional type

<pre snippet="datasets/datasets#invalid_user_dataset_optional_key_field" />


Invalid - no field of `datetime` type

<pre snippet="datasets/datasets#invalid_user_dataset_no_datetime_field" />


Invalid - no explicitly marked `timestamp` field
and multiple fields of type `datetime`, hence the timestamp 
field is ambiguous
<pre snippet="datasets/datasets#invalid_user_dataset_ambiguous_timestamp_field" />

Valid - even though there are multiple datetime fields, one of 
them is explicitly annotated as timestamp field.
<pre snippet="datasets/datasets#valid_dataset_multiple_datetime_fields" />

### Meta Flags

Datasets can be annotated with useful meta information via 
[metaflags](/governance/metaflags) - either at the dataset level or at the 
single field level. To ensure code ownership, Fennel requires every dataset to
have an owner. Here is an example:

<pre snippet="datasets/datasets#metaflags_dataset" />
