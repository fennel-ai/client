---
title: Operators
order: 4
status: WIP
---

# Operators

Operators are the building blocks of Fennel pipelines. More formally, they are 
functions that take one or more datasets as inputs and return another 
dataset as the output.


### Assign

Fennel lets you assign a value column or create a new column by using the `assign` operator.
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

### Groupby/ Window

In Fennel, the `window` operator enables grouping events into windowed streams based on specified criteria. A common application of this operator is in creating sessions, where it forms a window comprising events that occur in a session.
The `window` operator must be preceded by `groupby` to identify the fields that will be used for grouping the events before they are windowed.

`groupby` takes the following parameters:

1. `*args: str` - positional arguments that specify the list of fields to group by.

The `window` operator takes the following parameters:

1. ``type: str``: positional argument, that specifies the type of windows to create. Allowed types
   are `session`, `tumble` and `sliding`.
2. ``gap: str``: positional argument, that specifies the maximum time gap between two consecutive events for clubbing them into a single window.
3. ``field: str``: positional argument, that specifies the name of the keyed window field. The window field will contain following information: begin time, end time and number of events in a particular window.

<pre snippet="api-reference/operators_ref#window"></pre>

:::info
The `tumble` and `sliding` window type are still in development phase.
:::