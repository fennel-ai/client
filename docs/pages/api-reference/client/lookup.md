---
title: Lookup
order: 0
status: published
---

### Lookup
Method to lookup rows of keyed datasets.

#### Parameters
<Expandable title="dataset_name" type="str">
The name of the dataset to be looked up.
</Expandable>

<Expandable title="keys" type="List[Dict[str, Any]]">
List of dict where each dict contains the value of the key fields for one row 
for which data needs to be looked up.
</Expandable>

<Expandable title="fields" type="List[str]">
The list of field names in the dataset to be looked up.
</Expandable>

<Expandable title="timestamps" type="List[Union[int, str, datetime]]" defaultVal="None">
Timestamps (one per row) as of which the lookup should be done. If not set, 
the lookups are done as of now (i.e the latest value for each key).

If set, the length of this list should be identical to that of the number of elements
in the `keys`.

Timestamp itself can either be passed as `datetime` or `str` (e.g. by using 
`pd.to_datetime` or `int` denoting seconds/milliseconds/microseconds since epoch).
</Expandable>

<pre snippet="api-reference/client/lookup#basic" status="success"
    message="Example of doing lookup on dataset" highlight="27-38">
</pre>
