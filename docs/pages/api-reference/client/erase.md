---
title: Erase
order: 0
status: published
---
`erase`
### Erase

Method to hard-erase data from a dataset.

Data related to the provided erase keys is removed and will not be reflected to 
downstream dataset or any subsequent queries.

This method should be used as a way to comply with GDPR and other similar 
regulations that require "right to be forgotten". For operational deletion/correction
of data, regular CDC mechanism must be used instead.

:::warning
Erase only removes the data from the dataset in the request. If the data has
already propagated to downstream datasets via pipelines, you may want to issue
separate erase requests for all such datasets too.
:::


#### Parameters
<Expandable title="dataset" type="Union[Dataset, str]">
The dataset from which data needs to be erased. Can be provided either as 
a Dataset object or string representing the dataset name.
</Expandable>

<Expandable title="erase_keys" type="pd.Dataframe">
The dataframe containing the erase keys - all data matching these erase keys is 
removed. The columns of the dataframe must have the right names & types to be 
compatible with the erase keys defined in the schema of dataset.
</Expandable>

<pre snippet="api-reference/client/erase#basic" status="success"
    message="Erasing data corresponding to given uids">
</pre>
