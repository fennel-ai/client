---
title: 'Pipelines'
order: 3
status: 'published'
---

# Pipelines

Once you have defined datasets and sourced them them from external datasets, you might want to derive new datasets from existing datasets. That is where pipelines come in! Pipelines are functions defined on a dataset that describe how a Dataset can be derived from one or more existing Datasets. Let's look at a quick example:

### Example

Imagine we have the following datasets defined in the system:

<pre snippet="datasets/pipelines#data_sets" />


And we want to create a dataset which represents some stats about the transactions made by a user in a country different from their home country. We'd write that dataset as follows:

<pre snippet="datasets/pipelines#pipeline" />


There is a lot happening here so let's break it down line by line:

Lines 1-6 are defining a regular dataset - hopefully you are comfortable with these by now. Just note that this dataset has the schema that we desire to create.

Lines 10-17 describe the actual pipeline code - we'd come to that in a second. Line 9 declares that this is a classmethod - all pipelines are classmethods. `pipeline` decorator itself wraps `classmethod` decorator so you can omit `classmethod` in practice - here it is shown for just describing the concept. And line 8 declares that the following code represents a pipeline - all pipelines are decorated with `pipeline` decorator. This decorator takes the names of one or more existing datasets - these are the base datasets using which new datasets will be derived. In this case, this pipeline is declaring that it starts from `User` and `Transaction` datasets.

Now if you carefully inspect the signature of the pipeline function in line 10, you'd notice that it takes 2 arguments besides `cls` - they are essentially symbols for `User` dataset and `Transaction` dataset respectively.&#x20;

The function body is able to do manipulate these symbols and create other dataset objects. For instance, line 11 joins these two datasets and the resulting dataset is stored in variable `joined`. Line 12 does a `filter` operation on `joined` and stores the result in another dataset called `abroad`. Finally lines 13-17 aggregate the `abroad` dataset and create a dataset matching the schema defined in lines 3-6.&#x20;

That's your first pipeline in broad strokes but there is still lot more to unpack. Let's dive deep to get a deeper understanding of what is actually happening here.&#x20;

### Interplay with Pandas Dataframes

You might have noticed that the line 12 in the above example takes a lambda with a parameter called `df` - if you guessed it to be a pandas dataframe, you guessed it exactly right!&#x20;

Fennel pipeline topology is defined via a handful of operators like `filter`, `transform`, `join` etc. and some of these operators let you specify the actual logic using Python functions. For these Python functions, all input and output variables are Pandas Dataframe or Pandas Series. Here is an example with `transform` operator demonstrating this:

<pre snippet="datasets/pipelines#transform_pipeline" />


Here is line 20, the pipeline is running a transform operator and is passing a regular Python function to that (which is defined in line 12-17). The input and output of that function are both Pandas dataframes and within the body of that function, you can carry out arbitrary Pandas operations. This interplay with Pandas allows you to define very complex pipelines in familiar libraries instead of having to run a new DSL.

### **How do the pipelines actually work?**

When a sync call is made, Fennel client parses all the pipelines on all the datasets and runs those functions right there (which is possible since they are classmethods with no state) - the output of the pipeline function is interpreted as an AST describing the pipeline topology. This pipeline topology is sent to Fennel servers where the server type-checks the pipeline nodes and materializes a herd of jobs. Each such job is a continuous event loop waiting for new data to arrive before doing their computation and forwarding it to other jobs until the data reaches the destination dataset.&#x20;

### Execution: Streaming ~~Vs~~ And Batch

The pipelines declaratively specify what should happen with the data without getting into the weeds of how it will run, how it will be partitioned/scale etc. Fennel "compiles" the pipeline and materializes various "jobs" that are needed for the pipeline to work and manages their state/scaling etc automatically.&#x20;

One of the most powerful aspects of pipelines is that the same pipeline definition will work no matter what is the source of the datasets. In the above example, for instance, `User` dataset could come from a batch source and `Transaction` dataset could come from say a streaming Kafka source and it will work exactly the same way.

In more technical terms, Fennel is built on top of [Kappa architecture](https://www.kai-waehner.de/blog/2021/09/23/real-time-kappa-architecture-mainstream-replacing-batch-lambda/) and models both the realtime and the batch cases as streaming computation.&#x20;

### Operators

Fennel supports a handful of very general purpose operators which together form the building blocks of any pipeline. You can read about all the operators [here](/datasets/pipelines#operators). Further, transform operator takes free-form Python using which arbitrary computation can be done (including making calls into external services if needed).

The ONLY constraint on the pipeline topology is that `aggregate` has to be the terminal node i.e. it's not allowed to compose any other operator on the output of `aggregate` operator. This very limited constraint allows Fennel to significantly reduce costs/perf of pipelines. And it's possible that even this constraint will be removed in the future.&#x20;

### Schema Propagation Via Pipelines

Whenever a new pipeline is first synced with the server, Fennel inspects schemas of all the datasets and verifies that they are mutually compatible all the way from input datasets to the destination datasets (with the exception of transform operator body - see [here](/datasets/pipelines#operators) for details). As a result, Fennel is able to catch any schema mismatch errors at sync time itself.

### **Nested Pipelines**

It's completely valid to write pipelines where the input datasets themselves have a pipeline in their definition. For instance, imagine we have four datasets - D1, D2, D3, and D4. D1 is somehow sourced from an external dataset. D2 is derived from D1 via a pipeline. And both D3 and D4 are derived from D2 via their own pipelines - this is valid and normal. In fact, this pattern can be used to reduce costs by computing the intermediate datasets only once. In this example, for instance, D2 is created only once and reused in both D3 and D4.&#x20;

### **Multiple pipelines**

It is valid to have a dataset with multiple pipelines - in such a case, all the pipelines are independently run and the destination dataset is a union of all their outputs.&#x20;

Here is an example:

<pre snippet="datasets/pipelines#multiple_pipelines" />

Here imagine that we have two different datasets, potentially with their own separate external sources - corresponding to login activity on Android and iOS devices. And we want to create a dataset that "merges" rows from both, just tagged with the platform name. That can be done by having a dataset with two pipelines - in this example `android_logins` and `ios_logins`.&#x20;

### **Implementing Lambda Architecture Via Multiple Pipelines**

Generally speaking, Fennel itself follows [Kappa architecture](https://www.kai-waehner.de/blog/2021/09/23/real-time-kappa-architecture-mainstream-replacing-batch-lambda/) and expresses all computation via streaming. But it can be trivially used to implement [lambda architecture](https://www.databricks.com/glossary/lambda-architecture) for your usecases.&#x20;

Imagine you have a realtime source (say in Kafka) and a batch source (say in Snowflake) that is batch corrected every night. And you'd like to do some computation using Kafka in realtime but also correct the data later when Snowflake data is available. That can be trivially done by writing a dataset having two pipelines - one of them can build on top of Kafka and work realtime. The other one can build on Snowflake and process the same data later.&#x20;

In this way, batch corrected data is put in the destination dataset later, and hence ends up "overwriting" the earlier realtime data from Kafka pipeline, hence giving you the full power of lambda architecture.&#x20;
