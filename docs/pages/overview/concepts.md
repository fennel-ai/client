---
title: 'Concepts'
order: 0
status: 'published'
---

# Concepts

Fennel has two main concepts -- datasets and featuresets. Let's look at both one by one

## 1. Dataset

Dataset refers to a "table" of data with typed columns. Duh! Here is how a dataset is defined.&#x20;

<pre snippet="overview/concepts#user_dataset" />

This dataset has four columns -- `uid` (of type int), `dob` (of type datetime),`country` (of type string), and `signup_time` (of type datetime). For now, ignore the `field(...)` descriptors - they'd be explained soon.&#x20;

How to get data into a dataset? It's possible to hydrate datasets from external data sources:

First we define the external sources by providing the required credentials:

```python
from fennel.sources import source, Postgres, Kafka

postgres = Postgres(host=...<credentials>..)
kafka = Kafka(...<credentials>..)
```

Then we define the datasets that will hydrate themselves from these sources:

<pre snippet="overview/concepts#external_data_sources" />

The first dataset will poll postgres table for new updates every minute and hydrate itself with new data. The second dataset hydrates itself from a kafka topic. Fennel supports connectors with all main sources - check [here](/datasets/sources) for details.&#x20;

Hydrating datasets this way from external sources already looks somewhat cool because it allows you to bring data from multiple places in the same abstraction layer. But what to do with these datasets?

Fennel lets you derive new datasets from existing datasets by writing simple declarative Python code - it's unimaginatively called a pipeline. Let's look at one such pipeline:

<pre snippet="overview/concepts#pipeline" />


This is a dataset that will keep rolling stats about transactions made by a user abroad and we want to derive it from `User` dataset and `Transaction` dataset. Line 8-17 define this pipeline. You'd note that this pipeline is written using native Python and Pandas so you can unleash the full power of Python. But more importantly, this pipeline is operating on two datasets, one of which is streaming (i.e. `Transaction` ) and comes from Kafka and the other is static-ish dataset (i.e. `User`) coming from Postgres. And you can do joins and aggregations across them both. Wow! Now this is beginning to look powerful. What else can you do with the datasets?

It's also possible to do low latency lookups on these datasets using dataset keys. Earlier you were asked to ignore the field descriptors -- it's time to revisit those. If you look carefully, line 3 above defines `uid` to be a key (dataset can have multi-column keys too). If we know the uid of a user, we can ask this dataset for the value of the rest of the columns. Something (but not exactly) like this:

<pre snippet="overview/concepts#dataset_lookup" />

Here "found" is a boolean series denoting whether there was any row in the dataset with that key. If data isn't found, a row of Nones is returned. What's even cooler is that this method can be used to lookup the value as of any arbitrary time -- Fennel datasets track time evolution of data as the data evolves and can go back in time to do a lookup. This movement of data is tagged with whatever field is tagged with `field(timestamp=True)`. In fact, this ability to track time evolution enables Fennel to use the same code to generate both online and offline features.&#x20;

Okay so we can define datasets, source them from external datasets, derive them via pipelines, and do complex temporal lookups on them. What has all this to do with features? How to write a feature in Fennel? Well, this is where we have to talk about the second main concept -- featureset

## 2. Featureset

A featureset, as the name implies, is just a collection of features, each with some code that knows how to extract it - called an _extractor_. That may sound like a mouthful but isn't that complicated. Let's define a feature that computes user's age using the datasets we defined above.

Here is how a really simple featureset looks:

<pre snippet="overview/concepts#featureset" />

This is a featureset with 3 features --- `uid`, `country`, and `age`. Lines 7-11 describe an extractor that given the value of the feature `uid`, knows how to define the feature `age` (this input/output information is encoded in the typing signature, not function names). Inside the extractor function, you are welcome to do arbitrary Python computation. Similarly, lines 13-16 define another extractor function, this time which knows how to compute `country` given the input `uid`.&#x20;

More crucially, these extractors are able to do lookup on `User` dataset that we defined earlier to read the data computed by datasets. That's it - that's the basic anatomy of a featureset - one or more typed features with some extractors that know how to extract those features. These features extractors can recursively depend on other features (whether in the same featureset or across) and know how to compute the output features.&#x20;

At this point, you may have questions about the relationship between featuresets and datasets and more specifically pipeline and extractor.&#x20;

The main difference is that datasets are updated on the write path -- as the new data arrives, it goes to datasets from which it goes to other datasets via pipelines. All of this is happening asynchronously and results in data being stored in datasets. Features, however, are a purely 'read side' concept - feature is extracted while the request is waiting (this could be online feature serving request or offline training data generation request). Features can recursively depend on other features. And the bridge between them is `lookup` functionality of the dataset.&#x20;

Here is a diagram of how the concepts fit together:

![Diagram](/assets/readwritepath.png)

This provides a relatively simplified bird's eye view of the main concepts. But there is more to both datasets and featuresets and how they come together. You can read in more detail about [datasets here](/datasets/overview) and about [featuresets here](/featuresets/overview).

## Syncing Datasets and Features with Fennel

When you work with Fennel, your datasets and featuresets will live in a Python file in your codebase and Fennel servers will not know about them until you inform the servers by issuing a `sync` call. Here is how it will look:

```python
from fennel.client import Client

client = Client(<FENNEL SERVER URL>)
client.sync(
    datasets=[User, Transaction, UserTransactionsAbroad],
    featuresets=[UserFeature],
)
```

Line 4 here makes a POST request to Fennel and syncs the dataset on the server. Fennel may reject this sync request if there is any error with any dataset or featureset e.g. if a dataset already exists with this name or somehow this dataset is malformed.&#x20;

Overtime, you'd have many more datasets and featuresets - you'd send all of them in a sync call. And with that, the validation can become lot more complex e.g schema compatibility validation across the whole graph of datasets/featuresets

Assuming the call succeeds, any datasets/featuresets that don't yet exist will be created, any datasets/featuresets that exist but are not provided in the sync call are deleted and rest are left unchanged. See the [section on CI/CD](/testing-and-ci-cd/ci-cd-workflows) to learn how the end to end deployment could work in a production environment

