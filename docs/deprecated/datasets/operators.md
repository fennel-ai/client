---
title: 'Operators'
order: 4
status: 'published'
---

# Operators

Fennel supports only the following operators, which can be composed in to express complex pipelines.

1. Filter
2. Transform
3. Join
4. Group By
5. Aggregate
6. Explode \[Coming Soon]

Let's dive deep in the concepts behind these. Meanwhile, you can look up detailed API specs for each of these [here](/api-reference/operators).

### Filter

Filter is used to filter some rows from a dataset. It takes a Python function with a single argument - a Pandas Dataframe and returns a boolean series of the same size as the number of rows in the dataframe. All indices where the returned value has True are kept and all others are discarded. Here is an example:

<pre snippet="datasets/operators#filter" />


### Transform

Transform is used to add, remove, or rename columns and takes a Python function with a single argument - a Pandas dataframe and returns back the resulting dataframe. To be able to track schemas, transform also needs to describe the expected schema of the output dataset. The only restriction on transform is that it can not modify key or timestamp columns - it can only add, remove, or rename non-key, non-timestamp columns (also called as 'value columns' within Fennel).&#x20;

Except for this constraint, it can do anything, including but not limited to hitting an API endpoint and enriching the dataframe with a column from that information. Here is an example:

<pre snippet="datasets/operators#transform" />


In this example, the transform operator in line 20 takes the function `rescale` and an expected output schema. Note that the key of the output dataset is still`movie` and the timestamp field is still `timestamp` even though that's not mentioned explicitly.

### Join

Join operator, as the name implies joins two datasets. It takes a list of field names that should be used for joining. Here is an example:

<pre snippet="datasets/operators#join" />

Here are some more notes and constraints on join:

* The right side dataset must be a named dataset, not an intermediate dataset
* The join can only be done on the keys of the right side i.e. right side dataset must have keys and the only join condition is equality with the keys of the right side
* The joined dataset has the same keys & timestamp fields as the left side (and it's okay if left side itself had no keys - in that case, joined dataset also doesn't have keys)
* Technically, semantics of this operator are that of `leftjoin` i.e. if there is no row corresponding to `pid` in the `Product` dataset, a row will still be emitted, it will just have `None` in the place of `seller_id.`As a result, the data types of new columns need to be optional. That is why `seller_id` in line 16 is `Option[int]` even though `seller_id` in `Product` dataset itself is just `int`

### Group By & Aggregate

Groupby and aggregate operators are used together to aggregate a dataset. Groupby selects the dimensions on which grouping should happen - and these become the key fields in the resulting dataset, and aggregate operator takes a bunch of aggregation definitions. Let's look at an example:

<pre snippet="datasets/operators#aggregate" />


Here line 14 first groups the ad click stream by user ids and then counts the number of clicks done ever as well as number of clicks done in a rolling window of 1 week. Since rolling window is continuously moving forward, the value of this cell can change continuously. This is an example where Fennel's ability to track data mutations with time come into play - lookup operations as of time t on `UserAdStats` will return the number of clicks in a 1 week rolling window ending at time t (with a small approximation to keep the computation tractable).&#x20;

Currently, the following aggregation types are supported:

* **Count**: counts the number of events
* **Sum**: sums up the values of a particular column
* **Avg**: calculating running average of a column

Following more aggregation types will be added soon:

* Min
* Max
* Last K
* Decay counts

### Explode \[Coming Soon]

Explode operator will work very similar to how it works in [Pandas](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.explode.html) and make it possible to convert a field of type `list[T]` into many rows with field type replaced with `T.`&#x20;
