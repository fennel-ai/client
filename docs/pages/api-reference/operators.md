---
title: Operators
order: 4
status: wip
---

# Operators

### Join

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

TODO

### Transform

TODO

### Groupby / Aggregate

TODO

### Explode

TODO
