---
title: 'Pipeline'
order: 0
status: 'published'
---

# Pipeline

A pipeline is a function defined on a dataset that describes how it can
be derived from one or more existing datasets. Let's look at an example.

Imagine we have the following datasets defined in the system:

<pre snippet="datasets/pipelines#datasets"></pre>


And we want to create a dataset which represents some stats about the
transactions made by a user in a country different from their home country.
We will write that dataset as follows:

<pre snippet="datasets/pipelines#pipeline" highlight="14-25"></pre>

There is a lot happening here so let's break it down line by line:

* Lines 5-11 are defining a regular dataset. Note that this dataset has the
 schema that we desire to create via the pipeline.
* Highlighted lines describe the actual pipeline code - we will come to that in a second.
* Line 13 declares that this is a classmethod - all pipelines are classmethods.
`pipeline` decorator itself wraps `classmethod` decorator so you can omit
`classmethod` in practice - here it is shown for just describing the concept.
* Line 14 declares that the decorated function represents a pipeline.
* Line 15 declares all the input datasets that are needed for this pipeline to
 derive the output dataset. In this case, this pipeline is declaring that it
 starts from `User` and `Transaction` datasets.
* Notice the signature of the pipeline function in line 16 - it takes 2 arguments
besides `cls` - they are essentially symbols for `User` dataset and `Transaction`
dataset respectively.
* Pipeline's function body is able to manipulate these symbols and create other dataset
objects. For instance, line 17 joins these two datasets and the resulting dataset
is stored in variable `joined`. Line 18 does a `filter` operation on `joined` and
stores the result in another dataset called `abroad`. Finally lines 21-25
aggregate the `abroad` dataset and create a dataset matching the intended schema.

That's it - your first Fennel pipeline! Now let's look at a few more related
ideas.

### Operators

Fennel pipelines are built out of a few general purpose operators like `filter`,
`transform`, `join` etc which can be composed together to write any pipeline.
You can read about all the operators [here](/api-reference/operators). 


### Native Python Interop

Some of the operators (e.g. `transform`, `filter`) accept free-form Python 
lambdas and can be used to do arbitrary computation (including making calls into 
external services if needed). You can also import/use your favorite Python 
libraries inside these lambdas - thereby extending interop to the full Python
ecosystem. For such Python based operators, input/outputs variables 
are Pandas DataFrames or Pandas Series. Here is an example with `filter` and
`assign` operator:

<pre snippet="datasets/pipelines#transform_pipeline" highlight="12, 16"></pre>


The only constraint on the pipeline topology is that `aggregate` has to be the
terminal node i.e. it's not allowed to compose any other operator on the output
of `aggregate` operator. This constraint allows Fennel to
significantly reduce costs/performance of doing continuous sliding aggregations. 
And it's possible that even this constraint will be removed in the future.


### Power of Fennel Pipelines

Fennel pipelines have a bunch of really desirable properties:

1. **Extremely declarative** - you don't need to specify where/how pipelines should
    run, how much RAM they should take, how should they be partitioned if datasets
    are too big etc.

2. **Python Native** - as mentioned above, it's possible to run arbitrary Python
   computation using any Python libraries.

3. **Realtime** - as soon as new data arrives in any input dataset, pipeline
   propagates the derived data downstream. The same mechanism works whether
   the input dataset is continuously getting data in realtime or if it gets data
   in a batch fashion. The pipeline code is same in both realtime and batch cases.

4. **Immutable/versioned** - it's not possible to modify pipeline code unless explicitly
   declaring the intent to make changes. As a result, situations where half the data
   was derived using an older undocumented version of code never happen.

5. **Auto-backfilled** - pipelines are backfilled automatically on declaration. There
   is no separate backfill operation.
