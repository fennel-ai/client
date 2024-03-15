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
with the `@dataset` decorator. Let's look at an example:

<pre snippet="datasets/datasets#user_dataset"></pre>

### Dataset Schema

A dataset has few fields (interchangeably referred to as columns throughout the
documentation) with types and unique names. Each field must have has a 
pre-specified datatype. See the [typing](/api-reference/data-types/core-types) 
section to learn the types supported by Fennel.

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

<pre snippet="datasets/datasets#valid_user_dataset" status="success"
    message="Sole datetime field as timestamp field, okay to have no keys">
</pre>


<pre snippet="datasets/datasets#invalid_user_dataset_optional_key_field"
    status="error" message="Key fields can not have optional type"
    highlight="7">
</pre>


<pre snippet="datasets/datasets#invalid_user_dataset_no_datetime_field"
    status="error" message="No datetime field, so no timestamp field">
</pre>


<pre snippet="datasets/datasets#invalid_user_dataset_ambiguous_timestamp_field"
status="error" message="Multile datetime fields without explicit timestamp field"
    highlight="9,10">
</pre>

<pre snippet="datasets/datasets#valid_dataset_multiple_datetime_fields"
    status="success" message="Multiple datetime fields but one explicit timestamp field"
    highlight="9">
</pre>

### Versioning

All Fennel datasets are versioned and each version is immutable. The version of 
a dataset can be explicitly specified in the `@dataset` decorator as follows:

<pre snippet="datasets/datasets#dataset_version"></pre>

Increasing the version of a dataset can be accompanied with any other changes -
changing schema, changing source, change pipeline code etc. In either scenario,
Fennel recomputes the full dataset when the version is incremented.

The version of a dataset also captures all its ancestory graph. In other words, 
when the version of a dataset is incremented, the version of all downstream 
datasets that depend on it must also be incremented, leading to their 
reconstruction as well. 

As of right now, Fennel doesn't support keeping two versions of a dataset alive
simultaneously and recommends to either create datasets with differet names or
run two parallel branches with different versions of the dataset.

### Meta Flags

Datasets can be annotated with useful meta information via 
[metaflags](/data-quality/metaflags) - either at the dataset level or at the 
single field level. To ensure code ownership, Fennel requires every dataset to
have an owner. Here is an example:

<pre snippet="datasets/datasets#metaflags_dataset" highlight="4"></pre>

Typing the same owner name again and again for each dataset can get somewhat
repetitive. To prevent that, you can also specify an owner at the module level
by specifying `__owner__` and all the datasets in that module inherit
this owner. Example:

<pre snippet="datasets/datasets#metaflags_dataset_default_owners" highlight="4"></pre>

In this example, datasets `UserBMI` and `UserLocation` both inherit the owner
from the module level `__owner__` whereas dataset `UserName` overrides it by
providing an explicit meta flag.