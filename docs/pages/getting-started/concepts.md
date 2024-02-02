---
title: 'Concepts'
order: 5
status: 'published'
---

# Concepts

Fennel has two core concepts -- datasets and featuresets. Let's look at both one by one

## Dataset

Dataset refers to a "table" of data with typed columns. Here is how a dataset is defined.&#x20;

<Expandable title="inputs" type="List[Union[Feature, str]]">
List of features to be used as inputs to extract. Features should be provided 
either as Feature objects or strings representing fully qualified feature names.
</Expandable>

<pre snippet="overview/concepts#user_dataset"></pre>

This dataset has four columns -- `uid` (of type int), `dob` (of type datetime),`country` (of type string), and `signup_time` (of type datetime). For now, ignore the `field(...)` descriptors - they'd be explained soon.&#x20;

How to get data into a dataset? It's possible to hydrate datasets from external data sources:

First we define the external sources by providing the required credentials:

```python
from fennel.sources import source, Postgres, Kafka

postgres = Postgres(host=...<credentials>..)
kafka = Kafka(...<credentials>..)
```

Then we define the datasets that will hydrate themselves from these sources:

<pre snippet="overview/concepts#external_data_sources"></pre>

The first dataset will poll postgres table for new updates every minute and 
hydrate itself with new data. The second dataset hydrates itself from a kafka 
topic. Fennel supports connectors with all main sources - check 
[here](/concepts/source) for details.


Once you have one or more "sourced" datasets, you can derive new datasets from 
existing datasets by writing simple declarative Python code - it's 
unimaginatively called a pipeline. Let's look at one such pipeline:

<pre snippet="overview/concepts#pipeline"></pre>


It's possible to do low latency lookups on these datasets using dataset keys. 
Earlier you were asked to ignore the field descriptors -- it's time to revisit 
those. If you look carefully, line 4 with `field(key=True)` above defines `uid` 
to be a key (dataset can have multi-column keys too). If we know the uid of a 
user, we can ask this dataset for the value of the rest of the columns:

<pre snippet="overview/concepts#dataset_lookup"></pre>

Here "found" is a boolean series denoting whether there was any row in the 
dataset with that key. If data isn't found, a row of Nones is returned. What's 
even cooler is that this method can be used to lookup the value as of any 
arbitrary time (via the `ts` argument). Fennel datasets track time evolution of 
data as the data evolves and can go back in time to do a lookup. This movement 
of data is tagged with whatever field is tagged with `field(timestamp=True)`. In 
fact, this ability to track time evolution enables Fennel to use the same code 
to generate both online and offline features.


So we can define datasets, source them from external datasets, derive them 
via pipelines, and do temporal primary key lookups on them. What has all this to do 
with features? How to write a feature in Fennel? Well, this is where we have to 
talk about the second main concept - featureset.

## Featureset

A featureset, as the name implies, is just a collection of features. Features in 
Fennel, however, are different from features in many other systems - they don't 
represent stored data but instead are backed by a stateless Python function that 
knows how to extract it - called an _extractor_. Let's define a feature that 
computes user's age using the datasets we defined above.

Here is how a really simple featureset looks:

<pre snippet="overview/concepts#featureset"></pre>

This is a featureset with 3 features --- `uid`, `country`, and `age`. Lines 8-14 
describe an extractor that given the value of the feature `uid`, knows how to 
define the feature `age`. Inside the extractor function, you are welcome 
to do arbitrary Python computation. Similarly, lines 16-21 define another 
extractor function, this time which knows how to compute `country` given the 
input `uid`.


More crucially, these extractors are able to do lookup on `User` dataset that 
we defined earlier to read the data computed by datasets. 

Datasets & featuresets have a complimentary relationship. Datasets are updated 
on the write path -- as the new data arrives, it goes to datasets from which it 
goes to other datasets via pipelines. All of this is happening asynchronously 
and results in data being stored in datasets. 

Features, however, are a purely 'read side' concept - feature is extracted while 
the request is waiting (this could be online feature serving request or offline 
training data generation request). And the bridge between them is `lookup` 
functionality of the dataset.


## Syncing Datasets and Features with Fennel

When you work with Fennel, your dataset and featureset definitions will live 
in a Python file in your codebase or a notebook - but the Fennel servers won't
know about them until you explicitly tell them. 

To communicate with Fennel server, you'd typically create a `Client` object:

<pre snippet="overview/concepts#client"></pre>

And once you have a client object, you'd issue a `sync` request to sync your 
dataset/featureset definitions with the server:

<pre snippet="overview/concepts#sync"></pre>

This makes a POST request to Fennel and syncs the dataset on the server. Fennel 
may reject this sync request if there is any error with any dataset or 
featureset e.g. if a dataset already exists with this name or somehow this 
dataset is malformed.


Overtime, you'd have many more datasets and featuresets - you'd send all of them
in a sync call. And with that, the validation can become lot more complex e.g 
schema compatibility validation across the whole graph of datasets/featuresets.

Assuming the call succeeds, any datasets/featuresets that don't yet exist will 
be created, any datasets/featuresets that exist but are not provided in the sync 
call are deleted and rest are left unchanged. 

## Feature Extraction Requests

Once a few datasets/featuresets have been defiend, you'd want to read the value 
of these features for particular inputs (say uids). That can be accomplished via
`extract` API which reads the 'latest' value of features or `extract_historical`
API which reads historical values of features for training data generation
purposes. Here is how they look:

<pre snippet="overview/concepts#query"></pre>

Here, we are trying to read two features of the `UserFeature` featureset - `age` 
and `country` (as defined in `outputs`). We are providing the value of one
feature `uid` (as defined in `inputs`) and the actual values of `uid` are 
provided in `input_dataframe` object. Note that even though this snippet shows
Python, this is a REST endpoint and can be queried via any other language as well.

There is a parallel function to get historical values of features called 
`extract_historical`:

<pre snippet="overview/concepts#query_historical"></pre>

This is almost identical as before except we are also passing row-level timestamps
as of which we want features to be extracted. `extract_historical` is the mechanism
to generate point-in-time correct training datasets or do large scale batch inference.


### Learn more

Here are a few topics to read next to learn more:

<Grid>
	<PageReference href="/concepts/source/" illustration="/assets/illustrations/sources.svg" title="Sources">
		Ingest data from external data sources to populate Fennel datasets.
	</PageReference>

	<PageReference href="/concepts/pipeline/" illustration="/assets/illustrations/pipelines.svg" title="Pipelines">
		Create pipelines to derive new datasets from existing datasets.
	</PageReference>

	<PageReference href="/concepts/featureset/" illustration="/assets/illustrations/lookups.svg" title="Featuresets">
		Define features as featuresets using datasets
	</PageReference>
</Grid>