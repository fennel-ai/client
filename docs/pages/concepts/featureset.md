---
title: 'Featureset'
order: 4
status: 'published'
---

# Featureset

Featuresets refer to a group of logically related features where each feature is
backed by a Python function that knows how to extract it. A featureset is written
as a Python class annotated with `@featureset` decorator. A single application
will typically have many featuresets. Let's see an example:
<pre snippet="featuresets/overview#featureset"></pre>

Above example defines a featureset called `Movie` with two features - `duration`,
`over_2hrs`. Each feature has a [type](/api-reference/data-types/core-types) and 
is given a monotonically increasing `id` that is unique within the featureset. 
This featureset has one extractor - `my_extractor` that when given the `duration`
feature, knows how to extract the `over_2hrs` feature. 

## Extractors
Extractors are stateless Python functions in a featureset that are annotated
by `@extractor` decorator. Each extractor accepts zero or more inputs
(marked in `inputs` decorator) and produces one or more features (marked in
`outputs` decorator).

In the above example, if the value of feature `durations` is known, `my_extractor`
can extract the value of `over_2hrs`. When you are interested in getting values of
some features, Fennel locates the extractors responsible for those features, verifies
their inputs are available and runs their code to obtain the feature values.

### API of Extractors
Conceptually, an extractor could be thought of as getting a table of timestamped
input features and producing a few output features for each row. Something like
this (assuming 3 input features and 2 output features):

| Timestamp             | Input 1 | Input 2  | Input 3 | Output 1 | Output 2 |
| --------------------- | ------- | -------- | ------- | -------- | -------- |
| Jan 11, 2022, 11:00am | 123     | 'hello'  | True    | ?        | ?        |
| Jan 12, 2022, 8:30am  | 456     | 'world'  | True    | ?        | ?        |
| Jan 13, 2022, 10:15am | 789     | 'fennel' | False   | ?        | ?        |

The output is supposed to be the value of that feature as of the given timestamp
assuming other input feature values to be as given.

Extractor is a classmethod so its first argument is always `cls`. After that, the
second argument is always a series of timestamps (as shown above) and after that,
it gets one series for each input feature. The output is a named series or dataframe,
depending on the number of output features, of the same length as the input features.

### Validity Rules of Extractors
1. A featureset can have zero or more extractors.
2. An extractor can have zero or more inputs but must have at least one output.
3. Input features of an extractor can belong to any number of featuresets but all
   output features must be from the same featureset as the extractor.
4. For any feature, there can be at most one extractor where the feature
   appears in the output list.

With this, let's look at a few valid and invalid examples:

<pre snippet="featuresets/overview#featureset_zero_extractors"
   status="success" message="Featureset can have zero extractors">
</pre>

A featureset having a feature without an extractor simply means that Fennel 
doesn't know how to compute that feature and hence that feature must always be
provided as an input to the resolution process.

<pre snippet="featuresets/overview#featureset_many_extractors"
   status="success" 
   message="Can have multiple extractors for different features">
</pre>

<pre snippet="featuresets/overview#featureset_extractors_of_same_feature"
   status="error" 
   message="Multiple extractors extracting the same feature over_3hrs">
</pre>

<pre snippet="featuresets/overview#remote_feature_as_input"
   status="success" message="Input feature coming from another featureset">
</pre>

<pre snippet="featuresets/overview#remote_feature_as_output"
   status="error" message="Extractor outputs feature from another featureset"
></pre>

## Extractor Resolution
Support you have an extractor `A` that takes feature `f1` as input and outputs `f2`
and there is another extractor `B` that takes `f2` as input and returns `f3` as 
output. Further, suppose that the value of `f1` is available and you're interested
in computing the value of `f3`.

Fennel can automatically deduce that in order to go from `f1` to `f3`, it 
must first run extractor `A` to get to `f2` and then run `B` on `f2` to get `f3`. 

More generally, Fennel is able is able to do recursive resolution of feature 
extractors and find a path via extractors to go from a set of input features
to a set of output features. This allows you to reuse feature logic and not have
every feature depend on root level inputs like uid.

## Dataset Lookups
A large fraction of real world ML features are built on top of stored data.
However, featuresets don't have any storage of their own and are completely
stateless. Instead, they are able to do random lookups on datasets and use
that for the feature computation. Let's see an example:

<pre snippet="featuresets/reading_datasets#featuresets_reading_datasets"></pre>
In this example, the extractor is just looking up the value of `name` given the
`uid` from the `User` dataset and returning that as feature value. Note a couple
of things:
* Extractor has to explicitly declare that it depends on `User` dataset.
  This helps Fennel build an explicit lineage between features and the datasets they
  depend on.
* Inside the body of the extractor, function `lookup` is invoked on the dataset. 
 This function also takes series of timestamps as the first argument - you'd 
 almost always pass the extractor's timestamp list to this function as it is. 
 In addition, all the key fields in the dataset become kwarg to the lookup function.
* It's not possible to do lookups on dataset without keys.

## Versioning
Fennel supports versioning of features. Since a feature is defined
by its extractor, feature versioning is managed by setting `version` on extractors.

<pre snippet="featuresets/overview#feature_versioning"></pre>

If not specified explicitly, versions in Fennel begin at 1. Feature/extractor
details can not be mutated without changing the version. To update the 
definition of a feature, its extractor must be replaced with another
extractor of a large version number. For instance, the above featureset can be 
updated to this one without an error:

<pre snippet="featuresets/overview#feature_versioning_increment"></pre>

## Auto Generated Extractors
Fennel extractors are flexible enough to mix any combination of read & write
side computations. In practice, however, the most common scenario is computing
something on the write-side in a pipeline and then serving it as it is. Fennel
supports a shorthand annotation to auto-generate these formulaic extractors. 

<pre snippet="featuresets/overview#featureset_auto_extractors"></pre>

In the above example, the extractor for the feature `duration` is auto-generated
via the `feature(...)` initializer. In particular, the first expression to the 
`feature` function, in this case, a field of the dataset defined above, describes
what the extractor should result in. This snippet roughly generates code that 
is equivalent to the following:

<pre snippet="featuresets/overview#auto_expanded"></pre>

The way this works is that Fennel looks at the dataset whose field is being
referred to and verifies that it is a keyed dataset so that it can be looked up.
It then verifies that for each key field (i.e. `id` here), there is a feature 
in the featureset with the same name. If `default` is setup, logic equivalent to
`fillna` line is added and the types are validated.

As shown above, auto-generated extractors can be arbitrarily mixed with manually 
written extractors - with subset of extractors written manually and the rest derived in
an automatic manner.

### Feature Aliases
Above, we saw how to auto generate an extractor that looks up a field of a keyed
dataset. Fennel also lets you create aliases of features via auto-generated 
extractors.

<pre snippet="featuresets/overview#featureset_alias"></pre>

Two extractors are auto-generated in the above example - one that looks up field
of the dataset, as before. But we are now generating a second extractor which simply
aliases feature `Request.movie_id` to `MovieFeatures.id` that roughly 
generates code equivalent to the following:

<pre snippet="featuresets/overview#featureset_alias_expanded"></pre>

### Performance
Fennel completely bypasses Python for auto-generated extractors and is able to 
execute the whole computation in the Rust land itself. As a result, they are 
significantly faster (by >2x based on some benchmarks) and should be used 
preferably whenever possible.


### Convention
Most of your extractors will likely be auto-generated. To reduce visual clutter 
by repeatedly writing `feature`, a convention followed in the Fennel docs is to
import `feature` as `F`.

<pre snippet="featuresets/overview#featureset_auto_convention"></pre>