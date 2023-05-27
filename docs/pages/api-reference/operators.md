---
title: Operators
order: 4
status: wip
---

# Operators

### Join ( Left Join )

Fennel allows you to join two datasets using the `left_join` operator. You can only join a dataset
against another dataset which has keys defined. The join operation is performed on the keys of the RHS dataset.

The `left_join` operator has the following parameters:

1. `dataset: Dataset` - positional argument, that specifies the RHS Dataset.&#x20;
2. `on: List[str]` - kwarg that specifies the list of fields to join on.
3. `left_on: List[str]` - optional kwarg specifying the list of fields to join on for the LHS dataset.&#x20;
4. `right_on: List[str]` - optional kwarg specifying the list of fields to join on for the RHS dataset.&#x20;
5. `fields: List[str]` - This is an optional kwarg that specifies which fields need to be taken from the RHS dataset during the join operation. If not specified, Fennel will take all the non-join and non-timestamp fields in the RHS dataset.

:::info
One must either provide the `on` parameter or both the `left_on` & `right_on` parameters. If providing `left_on` and `right_on` their lengths should be same.
:::

:::info
The `on` or `right_on` fields specified should be keys in the RHS Dataset.
:::

### Filter

Fennel allows you to filter a dataset using the `filter` operator. The filter function is expected to return an series of booleans which are used to filter the dataset.

The `filter` operator has the following parameters:

1. `func: Callable[pd.DataFrame, pd.Series[bool]]` - positional argument, that specifies the filter function which is expected to return a series of booleans.;

### Transform

Fennel allows you to transform a dataset using the `transform` operator. 
The `transform` operator has the following parameters:

1. `func: Callable[pd.DataFrame, pd.DataFrame]` - positional argument, that specifies the transform function. It could be defined inline or could be a reference to a function defined elsewhere. The transform function should take a pandas dataframe as input and return a pandas dataframe as output. 
2. `schema: Dict[str, Type]` - optional kwarg that specifies the schema of the output dataset. If not specified, the schema of the input dataset is used. 

### Groupby / Aggregate

Fennel allows you to groupby and aggregate a dataset using the `aggregate` operator.
The api requires you to chain the `groupby` operator with the `aggregate` operator.
The aggregate operator first does a groupby operation on the dataset and then applies the aggregation function on the grouped dataset. 
The list of aggregate functions can be found in the [aggregations section](/api-reference/aggregations).

The `groupby` operator takes the following parameters:
1. `*args: str` - positional arguments that specify the list of fields to groupby on.

The `aggregate` operator has the following parameters:
1. `aggregates: List[Aggregation]` - positional argument, that specifies the list of aggregations to apply on the grouped dataset.
