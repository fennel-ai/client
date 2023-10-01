---
title: Operators
order: 4
status: WIP
---

# Operators

Operators in Fennel are functions that takes one or more datasets as inputs and return a dataset as output and are the
building
blocks of a pipeline. Then look very similar to the operators
in [pandas](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html).

### Join

Fennel allows you to join two datasets using the `join` operator. You can only join a dataset
against another dataset which has keys defined. The join operation is performed on the keys of the RHS dataset.

The `join` operator has the following parameters:

1. `dataset: Dataset` - positional argument, that specifies the RHS Dataset.&#x20;
2. `how: str` - required kwarg indicating whether the join should be an inner join (`how='inner'`) or a left-outer
   join (`how='left'`).
3. `on: List[str]` - optional kwarg that specifies the list of fields to join on.
4. `left_on: List[str]` - optional kwarg specifying the list of fields to join on for the LHS dataset.&#x20;
5. `right_on: List[str]` - optional kwarg specifying the list of fields to join on for the RHS dataset.&#x20;
6. `within: Tuple[Duration, Duration]` - optional kwarg specifying the time window within which the join should be
   performed.&#x20;
    1. The first value in the tuple represents how far back in time we can go, to perform a join. The term "forever"
       means that we can go infinitely back in time when searching for an event to join from the left-hand side (LHS)
       data.
    2. The second value in the tuple represents how far ahead in time we can go to perform a join. This is useful when
       you want to create a buffer duration where the RHS data of the join can come later.
       The default value for this parameter is `("forever", "0s")` which means that we can go infinitely back in time
       and
       the RHS data should be available for the event time of the LHS data.&#x20;

:::info
One must either provide the `on` parameter or both the `left_on` & `right_on` parameters. If providing `left_on`
and `right_on` their lengths should be same.
:::

:::info
The `on` or `right_on` fields specified should be keys in the RHS Dataset.
:::

:::info
For left joins, Fennel converts the schema of the RHS dataset to become optional during the join operation. This is
because the join
operation could result in null values for the RHS dataset.
:::

:::info
Fennel does not permit column name collisions during the join operation. Users should rename the columns in the RHS
dataset before performing a join.
:::

<pre snippet="api-reference/operators_ref#join"></pre>

### Filter

Fennel allows you to filter a dataset using the `filter` operator. The filter function is expected to return an series
of booleans which are used to filter the dataset.

The `filter` operator has the following parameters:

1. `func: Callable[pd.DataFrame, pd.Series[bool]]` - positional argument, that specifies the filter function which is
   expected to return a series of booleans.;

<pre snippet="api-reference/operators_ref#filter"></pre>

### Transform

Fennel allows you to transform a dataset using the `transform` operator.
The `transform` operator has the following parameters:

1. `func: Callable[pd.DataFrame, pd.DataFrame]` - positional argument, that specifies the transform function. It could
   be defined inline or could be a reference to a function defined elsewhere. The transform function should take a
   pandas dataframe as input and return a pandas dataframe as output.
2. `schema: Dict[str, Type]` - optional kwarg that specifies the schema of the output dataset. If not specified, the
   schema of the input dataset is used.

<pre snippet="api-reference/operators_ref#transform"></pre>

### Groupby / Aggregate

Fennel allows you to groupby and aggregate a dataset using the `aggregate` operator.
The api requires you to chain the `groupby` operator with the `aggregate` operator.
The aggregate operator first does a groupby operation on the dataset and then applies the aggregation function on the
grouped dataset.
The list of aggregate functions can be found in the [aggregations section](/api-reference/aggregations).

The `groupby` operator takes the following parameters:

1. `*args: str` - positional arguments that specify the list of fields to groupby on.

The `aggregate` operator has the following parameters:

1. `aggregates: List[Aggregation]` - positional argument, that specifies the list of aggregations to apply on the
   grouped dataset.

<pre snippet="api-reference/operators_ref#aggregate"></pre>

### Groupby/ First

Fennel allows you to groupby and get the first entry (by timestamp) in each group using the `first` operator.
The `first` operator must be preceded by `groupby` to identify the grouping fields.

`groupby` takes the following parameters:

1. `*args: str` - positional arguments that specify the list of fields to group by.

The `first` operator does not take any parameters.

The returned dataset's fields are the same as the input dataset, with the grouping fields as the keys.

<pre snippet="api-reference/operators_ref#first"></pre>

### Drop

Fennel allows you to drop columns from a dataset using the `drop` operator.

The `drop` operator has the following parameters:

1. `columns: List[str]` - positional argument, that specifies the list of columns to drop from the dataset.

:::info
Fennel does not allow you to drop keys or timestamp columns from a dataset.
:::


<pre snippet="api-reference/operators_ref#drop"></pre>

### Rename

Fennel allows you to rename columns in a dataset using the `rename` operator.

The `rename` operator has the following parameters:

1. `columns: Dict[str, str]` - positional argument, that specifies the mapping of old column names to new column names.

:::info
Fennel does not permit column name collisions post the rename operation. An error is thrown if the new column names are
already present in the dataset.
:::

### Dedup

Fennel allows you to discard duplicate entries from a dataset by using the `dedup` operator. A subset of columns,
specified by the 'by' parameter, can be used for identifying duplicates. Two entries are considered duplicates if and
only if they have the same values for the timestamp field and the `by` columns.

The `dedup` operator has the following parameters:

1. `by: Optional[List[str]]` - optional kwarg that specifies the list of fields to use for identifying duplicates. If
   not
   specified, all the columns (except the timestamp column) are used for identifying duplicates.

:::info
The `dedup` operator can only be applied on datasets without keys
:::

### Explode

Fennel allows you to explode `List` type columns into separate rows by using the `explode` operator.
The behavior of this operator is similar
to the `explode`function in [Pandas](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.explode.html).

The `explode` operator has the following parameters:

1. `columns: List[str]` - list of columns to explode. All the columns should be of type `List`. If the input column's
   type if `List[T]`, the output column is assigned the type `Optional[T]`.

:::info
The number of elements in each list should be the same for every row, although the lists can be of different lengths
across rows.
:::

:::info
The `explode` operator does not support exploding on key columns of datasets>
:::

<pre snippet="api-reference/operators_ref#rename"></pre>

### Assign

Fennel allows you to assign to a specific value column or create a new column by using the `assign` operator.
The `assign` operator has the following parameters:

1. ``column: str``: positional argument, that specifies the name of the column to assign to or create.
2. ``result_type: Type``: positional argument, that specifies the type of the column in the output dataset
3. ``func: Callable[pd.DataFrame, pd.DataFrame]``: positional argument, that specifies the assign function. 
        It could be defined inline or could be a reference to a function defined elsewhere. 
        The assign function should take a pandas dataframe as input and return a pandas dataframe as output.

<pre snippet="api-reference/operators_ref#assign"></pre>

:::info
Fennel does not allow you to assign keys or timestamp columns from a dataset.
:::


