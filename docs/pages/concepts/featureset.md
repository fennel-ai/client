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

### Example

<pre snippet="featuresets/overview#featureset"></pre>


Above example defines a featureset called `Movie` with two features - `duration`,
`over_2hrs`. Each feature has a [type](/api-reference/data-types) and is given
a monotonically increasing `id` that is unique within the featureset. This
featureset has one extractor - `my_extractor` that when given the `duration`
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

**API of Extractors**

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



**Rules of Extractors**

1. A featureset can have zero or more extractors.
2. An extractor can have zero or more inputs but must have at least one output.
3. Input features of an extractor can belong to any number of featuresets but all
   output features must be from the same featureset as the extractor.
4. For any feature, there can be at most one extractor where the feature
   appears in the output list.

With this, let's look at a few valid and invalid examples:

Valid - featureset can have zero extractors
<pre snippet="featuresets/overview#featureset_zero_extractors"></pre>

Valid - featureset can have multiple extractors provided they are all valid

<pre snippet="featuresets/overview#featureset_many_extractors"></pre>

Invalid - multiple extractors extracting the same feature. `over_3hrs` is
extracted both by e1 and e2:
<pre snippet="featuresets/overview#featureset_extractors_of_same_feature"></pre>

Valid - input feature of extractor coming from another featureset.
<pre snippet="featuresets/overview#remote_feature_as_input"></pre>

Invalid - output feature of extractor from another featureset
<pre snippet="featuresets/overview#remote_feature_as_output"></pre>

### Extractor Resolution

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

### Dataset Lookups

A large fraction of real world ML features are built on top of stored data.
However, featuresets don't have any storage of their own and are completely
stateless. Instead, they are able to do random lookups on datasets and use
that for the feature computation. Let's see an example:
<pre snippet="featuresets/reading_datasets#featuresets_reading_datasets"></pre>

In this example, the extractor is just looking up the value of `name` given the
`uid` from the `User` dataset and returning that as feature value. Note a couple
of things:
* In line 15, extractor has to explicitly declare that it depends on `User` dataset.
  This helps Fennel build an explicit lineage between features and the datasets they
  depend on.
* In line 19, extractor is able to call a `lookup` function on the dataset. This
 function also takes series of timestamps as the first argument - you'd almost always
 pass the extractor's timestamp list to this function as it is. In addition, all the
 key fields in the dataset become kwarg to the lookup function.
* It's not possible to do lookups on dataset without keys.


### Auto-generated Extractors
