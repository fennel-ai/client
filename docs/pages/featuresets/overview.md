---
title: Overview
order: 0
status: wip
---

# Overview

Featuresets refer to a group of logically related features where each feature is backed by a Python function that knows how to extract it. A featureset is written as a Python class annotated with `@featureset` decorator. A single application will typically have many featuresets.&#x20;

Let's look at an example to see how it looks:

### Example

<pre snippet="featuresets/overview#featureset" />


Above example defines a featureset called `Movie` with two features - `duration`, `over_2hrs`. Each feature has a [type](/api-reference/data-types) and is given a monotonically increasing `id` that is unique within the featureset. This featureset has one extractor - `my_extractor` that when given the `duration` feature, knows how to extract the `over_2hrs` feature. There is no extractor provided for `duration` feature - and that's okay. Every feature in a featureset can have either zero or one extractor.&#x20;

### Features

A featureset can have many features. Each feature is given an explicit type and an `id` that is unique within that featureset. Each feature can have zero or exactly one extractor function that is responsible for extracting the code. Features with zero extractors can not be extracted by Fennel and hence need to be provided as an `input` to the extraction process.&#x20;

### Extractors

Extractors are Python function that belong to a specific featureset and know how to extract one or more features of that featureset. Extractor functions are described by adding an `@extractor` decorator to the function code. Let's look at an example:

<pre snippet="featuresets/overview#featureset_extractor" />

**Extractor Signature**

Let's examine the signature of `func` more carefully - a lot of information is embedded in it. It takes two arguments - `ts` and `durations`. `ts` is of type `Series[datetime]`which is just the typing info for a Pandas series of timestamps (we will come to it in a minute). `durations` is of type `Series[duration]`which refers to a Pandas series of values of the feature `duration`. And this function returns a `Series[over_2hrs]` which refers to a Pandas series of values of the feature `over_2hrs`.

**Input Parameters**

1. Every extractor takes `ts: Series`, as the first positional argument. The `ts` parameter is used when doing a lookup operation on a Dataset. This parameter must be passed even if the extractor does not do any Dataset lookup.
2. One or more parameters that are of the form:
   * `Series[<feature>]`
   * `DataFrame[<featureset>]`

Series and DataFrame are pure syntactic sugar over `pd.Series` and `pd.DataFrame` and enable users to specify the exact feature/Featureset that the extractor expects to receive as input parameters, with the actual data type.&#x20;

**Return Annotation**

An extractor returns a `Series[<feature_name>]` if it is responsible for resolving a single feature or `Dataframe[<feature_1, feature_2, ..., feature_n>]` if it is responsible for resolving multiple features.&#x20;

If the extractor is resolving the entire Featureset, then return annotation is optional and can be left empty.&#x20;

Each feature in Fennel effectively becomes its own type (which can help prevent lots of bugs). With this context, the signature of extractor can now be seen as reading some features&#x20;

:::info
Although a single extractor can extract one or more features (including the entire featureset) every feature must have at most one extractor.&#x20;
:::

### Extracting Multiple Features

In the examples seen so far, extractors only extracted a single feature each. But it is possible for extractors to extract multiple features too. This is useful when multiple features are related and share lots of computation. Here is an example:

<pre snippet="featuresets/overview#multiple_feature_extractor" />


### Extractors Depending on Multiple Featuresets

In the examples seen so far, extractors depended only on the features of their own featureset. However, it is possible for extractors to depend on features from other featuresets too as inputs. This is actually very common - it's common for lots of features across many featuresets to depend on more foundational features e.g `uid` or `request_time.` Lets refactor the above example to use a common `uid` feature as part of a Request featureset:

<pre snippet="featuresets/overview#extractors_across_featuresets" />



