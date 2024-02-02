---
title: Explode
order: 0
status: published
---
# Explode

<Divider>
<LeftSection>
Operator to explode lists in a sinlge row to form multiple rows, analogous to 
to the `explode`function in [Pandas](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.explode.html).

Only applicable to keyless datasets.

#### Parameters

<Expandable title="columns" type="List[str]">

The list of columns to explode. 
This list can be passed either as unpacked *args or kwarg `columns` mapping to an
explicit list.

All the columns should be of type `List[T]` for some `T` in the input dataset and
after explosion, they get converted to a column of type `Optional[T]`.

</Expandable>

:::info
The number of elements in each list should be the same for every row, although 
the lists can be of different lengths across rows.
:::

</LeftSection>

<RightSection>
</RightSection>

</Divider>

TODO:
- valid case: as *args or kwargs
- invalid - exploding not list, exploding keyed dataset