---
title: Aggregations
order: 5
status: 'published'
---

# Aggregations

Aggregations are provided to the \`aggregate\` operator and specify how the agggregation should happen. All aggregations take two common arguments:

1. `window`: Window - argument that specifies the length of the duration across which Fennel needs to perform the aggregation. See how [duration](/api-reference/duration) is specified in Fennel.
2. `into_field`: str - the name of the field in the output dataset that corresponds to this aggregation. This&#x20;

Besides these common arguments, here is the rest of the API reference for all the aggregations:

### 1. Count

Count computes a rolling count for each group key across a window. Takes no additional arguments besides `window` and `into_field`. It returns 0 by default. Its output type is always `int`.&#x20;

### 2. Sum &#x20;

Sum aggregate computes a rolling sum across a window for a given field in the dataset. This field is specified by the `of` parameter of type `str`. If no data is available, the default value is 0. Its output type can be `int` or `float` depending on the input type (and any other input type will fail sync validation)

### 3. Average

Same as "Sum", but instead maintains a rolling average in the given window. In addition to the `of` field, also requires a `default` value to be specified which is returned when average is queried for a window with no data points. The input types can only be `int` or `float` and the output type is always `float`.

### 4. Min&#x20;

Same as "Sum", but instead maintains a rolling minimum in the given window. Requires a `default` value to be specified. Input type can be `int` or `float` and the output type is same as the input type.

### 5. Max&#x20;

Identical to "min", but instead maintains a rolling maximum in the given window.&#x20;

### 6. LastK

Maintains a list of "items" in the given rolling durations. If no events have been logged, returns an empty list. If input field is of type `T` the output field is of type `List[T]`