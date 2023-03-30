---
title: 'Lookups'
order: 2
description: The bridge between read and write paths
status: 'published'
---

# Lookups

As described in [Concepts Overview](/overview/concepts), Datasets updates are continuously computed and stored on the write path as new rows become available. But for all this data to be useful in feature engineering, feature extractors, which run on the read path, need to be able to read the data. Some sort of bridge is needed between the read path and the write path.&#x20;

That is where dataset lookups come in. The `lookup` function, as the name specifies, is used to lookup a dataset row for the given value of all the key fields. Let's look at an example:

<pre snippet="datasets/lookups#datasets_lookup" />

In this example, we have a dataset that knows the current city of the user and their home city. And we want to write a feature that checks if the user is currently in their home city or not. To do this, the extractor for the feature looks up the dataset in line 18. Note that the lookup method is directly called on the `User` class, not on any instance object. And the method takes two arguments in this case - let's look at both of them:

* The first one is a positional argument that describing a series of timestamps at which lookups happen. This argument will almost always be passed as it is from extractor signature to lookup and is set at the very top entry point depending on whether the extraction is online or for historical features.&#x20;
* The second one is a kwarg called `uid.`This expects a series of data that will be matched against `uid` field of User since that is a key field. If there were more key fields in the User dataset, the lookup method would have expected more kwargs, one for each key field of the dataset.

More specifically, the signature of the lookup method for a dataset`ds` with n key fields, has the following signature:

```python
def lookup(
    self, 
    ts: pd.Series[datetime],  
    <ds_key1>: pd.Series[<key1_dtype], 
    <ds_key2>: pd.Series[<key2_dtype],
    ... ,
    <ds_keyn>: pd.Series[<key1_dtype], 
    fields: List[str],
) -> Tuple[Dataframe, Series[bool]]
```

If you wanted to read only a few fields from the dataset, it's possible to specify that via the `fields` arguments in line 8.

Lookup method, like all other functional interfaces of Fennel, is batched to speed up cases when lots of data needs to be read at once. You're welcome to create a series with a single element and pass that if you want to read only one element.

### Return Types

The lookup method always returns a dataframe with the requested data and a series of booleans which denotes if the data was found or not. For instance, if there is no data in the dataset corresponding to the ith key, the ith element of this series will be `False`.&#x20;

The corresponding values in dataframe will be set to `None`. It is strongly advised (unlike this example), to explicitly check / handle the case when some data is not found. A common approach is to use `fillna` function of Pandas to substitute Nones with valid default values.

:::info
Under the hood, lookup calls for online extraction and historical extraction hit different codepaths. The former is optimized for latency and the latter for throughput.
:::
