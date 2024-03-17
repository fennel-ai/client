---
title: Window
order: 0
status: published
---
### Window
Operator to do aggregation of events into windows based on their timestamps. 
Window operator must always be preceded by a [groupby](/api-reference/operators#groupby) operator.

#### Parameters

<Expandable title="type" type='"hopping" | "tumbling" | "session"'>

The type of the window to be used.

Tumbling windows are fixed length and non-overlapping windows ensuring that each 
event is placed into exactly one window. With tumbling windows, `duration` must 
be set.

Hopping windows are also fixed width like tumbling but support overlapping 
windows, where a single event can be included in multiple 
windows. With hopping windows, `duration` and `stride` must be set.

Session windows are variable sized and are created based on the actual 
occurrence of events, with windows being at least `gap` duration apart. With
session windows, `gap` must be set.

See [this](https://www.databricks.com/blog/2021/10/12/native-support-of-session-window-in-spark-structured-streaming.html)
for a more detailed explanation of the three kinds of windows.
</Expandable>

<Expandable title="gap" type="Duration">
The timeout for inactivity that defines the boundary of a window. Events that 
occur closer together than the specified `gap` are grouped into the same window. 
Only relevant when `type` is set to `"session"`.
</Expandable>

<Expandable title="duration" type="Duration">
The total fixed length of the window. Only relevant when `type` is set to `"tumbling"`
or `"hopping"`.
</Expandable>

<Expandable title="stride" type="Duration">
The interval between the start of two successive windows. Only relevant when the
`type` is `"hopping"`. It is invalid for `stride` to be greater than the overall
`duration`.
</Expandable>

<Expandable title="into_field" type="str">
The name of the field in the output dataset that should store the result of this 
aggregation. This field is expected to be of type `"Window"`.
</Expandable>

<pre snippet="api-reference/operators/window#basic" status="success"
    message="Aggregate event into sessions that are 15 minutes apart">
</pre>

#### Returns
<Expandable type="Dataset">
Returns a dataset where all columns passed to `groupby` become the key columns 
along with a new key column of type `Window` that represents the window object.
The timestamp column of the input dataset stays the timestamp column in the output
dataset too and is used for windowing.
</Expandable>

#### Errors
<Expandable title="Setting incorrect parameters for the type of window">
Some parameters only work with a certain kind of window, make sure to check the 
documentation before going.
</Expandable>


<Expandable title="Using forever duration">
While `"forever"` is a valid duration and can be used for aggregation, it's not
a valid duration value for either tumbling or hopping window.
</Expandable>

<Expandable title="Missing window key">
This `into_field` field is expected to be a key and of type `Window`.
</Expandable>

<pre snippet="api-reference/operators/window#miss_window_key" status="error"
    message="Missing window key in schema">
</pre>

<pre snippet="api-reference/operators/window#invalid_window" status="error"
    message="Forever is not a valid duration, only valid duration allowed">
</pre>
