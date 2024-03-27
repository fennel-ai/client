---
title: Erase
order: 0
status: published
---

### Erase

Method to erase entity in a dataset based on erase key. 
Data related to erase keys issued will be remove from indices and will not be reflected to downstream dataset.
This method should be used as a way to comply to data regulation and not be used in an operational way.

*Note:* Downstream dataset that used the data before the erase key issued will not be cleaned up.


#### Parameters
<Expandable title="dataset" type="Union[Dataset, str]">
Dataset name to issue erase key to. Dataset should be provided either as Dataset objects or strings representing the dataset name.
</Expandable>

<Expandable title="keys" type="pd.Dataframe">
The dataframe containing all the erase key that must be deleted. The column of the 
dataframe must have the right names & types to be compatible with the erase key defined in the schemas of
datasets.
</Expandable>

<pre snippet="api-reference/client/erase#basic" status="success"
    message="Example of doing erase on dataset" highlight="27-38">
</pre>


