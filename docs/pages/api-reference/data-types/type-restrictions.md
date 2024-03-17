---
title: Type Restrictions
order: 3
status: 'published'
---

### Type Restrictions
Fennel type restrictions allow you to put additional constraints on 
base types and restrict the set of valid values in some form. 

<Expandable title="regex" type='regex("<pattern>")'>
Restriction on the base type of `str`. Permits only the strings matching the given 
regex pattern.
</Expandable>

<Expandable title="between" type='between(T, low, high)'>
Restriction on the base type of `int` or `float`. Permits only the numbers
between `low` and `high` (both inclusive by default). Left or right can be made
exclusive by setting `min_strict` or `max_strict` to be False respectively.
</Expandable>

<Expandable title="oneof" type='oneof(T, [values...])'>
Restricts a type `T` to only accept one of the given `values` as valid values.
`oneof` can be thought of as a more general version of `enum`.

For the restriction to be valid, all the `values` must themselves be of type `T`.
</Expandable>


#### Type Restriction Composition
These restricted types act as regular types -- they can be mixed/matched to 
form complex composite types. For instance, the following are all valid Fennel 
types:

* `list[regex('$[0-9]{5}$')]` - list of regexes matching US zip codes
* `oneof(Optional[int], [None, 0, 1])` - a nullable type that only takes 0 or 1 
  as valid values


:::info
Data belonging to the restricted types is still stored & transmitted (e.g. in 
json encoding) as a regular base type. It's just that Fennel will reject data of 
base type that doesn't meet the restriction.
:::

<pre snippet="api-reference/data-types#dataset_type_restrictions" />


