---
title: Data Types
order: 3
status: 'published'
---

# Data Types

Fennel supports the following data types, expressed as native Python type hints.

<TypesList>
	<TypesListRow types={["int", "float"]}>
		int is implemented as int64 and float is implemented as float64
	</TypesListRow>
	<TypesListRow types={["bool"]}>
		Booleans
	</TypesListRow>
	<TypesListRow types={["str"]}>
		Arbitrary sequence of bytes.
	</TypesListRow>
	<TypesListRow types={["List[T]"]}>
		List of elements of type `T`. Unlike Python lists, all elements must have the same type.
	</TypesListRow>
	<TypesListRow types={["dict[T]"]}>
		Map from `str` to data of type `T`. Please let us know if your use cases require dict with non-string keys
	</TypesListRow>
	<TypesListRow types={["Optional[T]"]}>
		Same as Python `Optional` - permits either `None` or values of type `T`
	</TypesListRow>
	<TypesListRow types={["Embedding[int]"]}>
		Denotes a list of floats of the given fixed length i.e. `Embedding[32]` describes a list of 32 floats. This is same as `list[float]` but enforces the list length which is important for dot product and other similar operations on embeddings.
	</TypesListRow>
	<TypesListRow types={["datetime"]}>
		Describes a timestamp, implemented as microseconds since Unix epoch (so minimum granularity is microseconds). Can be natively parsed from multiple formats.
	</TypesListRow>
</TypesList>


Note that types don't auto-typecast. For instance, if something was expected to
be of type `float` but received an `int`, Fennel will declare that to be an error.


### Type Restrictions

Imagine that you have a field that denotes a US zip code but stored as string. Not all strings denote valid zip codes - only those that match a particular regex do but this can be hard to encode, which can lead to incorrect data being stored.&#x20;

Fennel supports type restrictions -- these are additional constraints put on base types that restrict the set of valid values in some form. Here is a list of supported restrictions:

| Restricted Type         | Base Type                           | Restriction                                                                                                                                        |
| ----------------------- | ----------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `regex('<pattern>')`    | `str`                               | Permits only the strings matching the given regex pattern                                                                                          |
| `between(T, low, high)` | `T` where T can be `int` or `float` | Only permits values between low and high (both inclusive). Left or right can be made exclusive by setting `min_strict` or `max_strict` to be False |
| `oneof(T, [values...])` | `T`                                 | Only one of the given values is accepted. For the restriction to be valid, values themselves should be of type T                                   |



These restricted types act as regular types -- they can be mixed/matched to form complex composite types. For instance, the following are all valid Fennel types:

* `list[regex('$[0-9]{5}$')]` - list of regexes matching US zip codes
* `oneof(Optional[int], [None, 0, 1])` - a nullable type that only takes 0 or 1 as valid values

Note: data belonging to these restricted types is still stored / transmitted (e.g. in json encoding) as a regular base type. It's just that Fennel will reject data of base type that doesn't meet the restriction.

**Example:**

<pre snippet="api-reference/data-types#dataset_type_restrictions" />
