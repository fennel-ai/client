---
title: Window
order: 0
status: published
---
### Window

<Divider>
<LeftSection>
Operator to do aggregation of events into windows based on their timestamps. Window operator must always be preceded by a [groupby](/api-reference/operators#groupby) operator.

#### Parameters

<Expandable title="type" type="hopping | tumbling | session">

The type of window to group event on. Possible values are `"hopping"`, `"tumbling"`, `"session"`.

1. A `hopping` window is defined by two key parameters: duration and stride.
    
    - `duration`: The length of the window, indicating the time span that each window covers.
    
    - `stride`: The interval between the start of each window. This setting allows for the creation of overlapping windows, where a single event can be included in multiple windows. It's essential that the stride is less than the duration.

2. A `tumbling` window is characterized by a single parameter: duration.

    - `duration`: Specifies how long each window lasts. Unlike hopping windows, tumbling windows are designed to be non-overlapping and contiguous, ensuring that each event is placed into exactly one window, covering the entire data range without gaps.

3. A `session` window is dynamically sized based on event patterns, using the gap parameter.

    - `gap`: Determines the timeout for inactivity that defines the boundary of a window. Events that occur closer together than the specified gap are grouped into the same window. This approach allows windows to be created based on the actual occurrence of events, with windows being at least gap duration apart.

</Expandable>

<Expandable title="gap" type="Duration">

The parameters for `"session"` windows. Possible values are any [time duration](/api-reference/data-types/duration)

</Expandable>

<Expandable title="duration" type="Duration">

The parameters for `"tumbling"` or `"hopping"` windows. Possible values are any [time duration](/api-reference/data-types/duration)

</Expandable>

<Expandable title="stride" type="Duration">

The parameters for `"hopping"` windows. Possible values are any [time duration](/api-reference/data-types/duration)

</Expandable>

<Expandable title="into_field" type="str">
The name of the field in the output dataset that should store the result of this aggregation. This field is expected to be of type `"Window"`.
</Expandable>

#### Returns
<Expandable type="Dataset">

Returns a dataset where all columns passed to `groupby` become the key columns, the timestamp column become the end timestamps of the window corresponding to that aggregation. One key column is created of type `"Window"` to indicate the information of the window.
</Expandable>

#### Errors
<Expandable title="Setting incorrect parameters for the type of window">
Some parameters only work with a certain kind of window, make sure to check the documentation before going.
</Expandable>


<Expandable title="Invalid window">
This `window` field is different from `window` field in aggregation so `forever` is not valid as well as other invalid string
</Expandable>


<Expandable title="Missing window key">
This `into_field` field is expected to be a key and of type `Window`.
</Expandable>


</LeftSection>

<RightSection>
<pre snippet="api-reference/operators/window#basic" status="success"
    message="Aggregate event into sessions that are 15 minutes apart"
>
</pre>


<pre snippet="api-reference/operators/window#miss_window_key" status="error"
    message="Missing window key in schema"
>
</pre>


<pre snippet="api-reference/operators/window#invalid_window" status="error"
    message="Forever is not a valid duration, only valid duration allowed"
>
</pre>

</RightSection>
</Divider>

