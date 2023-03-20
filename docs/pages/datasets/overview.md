---
title: 'Overview'
order: 0
status: 'published'
---

# Overview

Datasets refer to a table like data with typed columns. Datasets can be sourced from external datasets (e.g. Kafka, Snowflake, Postgres etc.) or derived from other datasets.&#x20;

Datasets are written as [Pydantic](https://docs.pydantic.dev/) inspired Python classes decorated with the `@dataset` decorator.&#x20;

### Example

<pre snippet="datasets/datasets#user_dataset" />


### Dataset Schema

A dataset has few typed columns (interchangeably referred to as fields) and unique names. Each field must have has a pre-specified datatype. See the [typing](/api-reference/data-types.md) section to learn the types supported by Fennel.&#x20;

### Field Descriptors

You might have noticed the `field(...)` descriptor next to `uid` and `update_time` fields. These optional descriptors are used to provide non-typing related information about the field. In particular, there are three kinds of fields:

1. `key` fields - these are fields with `field(key=True)` set on them. The semantics of this are somewhat similar to those of primary key in relational datasets and implies that datasets can be looked-up by providing the value of key fields. It is okay to have a dataset with zero key fields - in those cases, it's not possible to do random lookups on the dataset at all. Typically realtime activity streams (e.g. click streams) will not have any key fields. It's also okay to have multiple key fields on a dataset - in that case, all of those need to be provided while doing a lookup. And since keys are tied to lookup, they can not be of an Optional type.
2. `timestamp` field - these are fields with `field(timestamp=True)`. Every dataset should have exactly one timestamp field and this field should always be of type `datetime`. Fennel datasets automatically track data mutations over time which is needed to be able to compute point-in-time correct features for training data generation. It's the value of the `timestamp` field that is used to associate a particular state of dataset row with a timestamp. While every dataset has exactly one timestamp field, it's possible to omit it in code - if a dataset has exactly one field with `datetime` type, it is assumed to be the timestamp field.&#x20;

:::info
Timestamp fields of datasets, in addition to time travel, also allows Fennel to handle out of order events and do time-windowed data aggregations
:::

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


### Meta Flags

Datasets can be annotated with useful meta information - either at the dataset level or at the single field level. And the same metaflags that work elsewhere in Fennel also work on datasets. Read more about [metaflags here](/governance/metaflags). Here is an example:

<pre snippet="datasets/datasets#metaflags_dataset" />

Quick note - to encourage code ownership, owner metaflag MUST be present on every dataset and featureset - else the sync call will fail. Fennel uses this to notify the owner of the Dataset in case of any issues (e.g. data quality problems, some upstream dataset getting marked deprecated etc.)

However, these are omitted in spirit of brevity and clarity in several examples throughout the documentation.&#x20;

### Learn more about datasets

Datasets, despite being a very simple and compact abstraction, pack a punch in terms of power. Here are a few topics to read next to learn more about datasets.&#x20;

<Grid>
	<PageReference href="/datasets/sources/" illustration="/assets/illustrations/sources.svg" title="Data Sources">
		Learn to pull data in from external data sources to populate a Dataset.
	</PageReference>

	<PageReference href="/datasets/pipelines/" illustration="/assets/illustrations/pipelines.svg" title="Pipelines">
		Create pipelines to derive new datasets from existing datasets.
	</PageReference>

	<PageReference href="/datasets/lookups/" illustration="/assets/illustrations/lookups.svg" title="Lookups">
		Read your datasets by doing lookups
	</PageReference>
</Grid>
