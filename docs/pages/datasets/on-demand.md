---
title: 'On Demand'
order: 5
status: 'published'
---

# On Demand

_NOTE: on demand functionality is still being developed and will be released in March 2023._

Datasets typically receive data from either an external source, or a pipeline, or both.&#x20;

However, sometimes fetching data can either be monetarily expensive (such as fetching the credit score from an external API vendor that charges per fetch) or computationally expensive (such as computing the BERT embedding using a heavily parameterized model). For such cases, moving the computation in a regular Pipeline may be infeasible. The alternative is to move them on the read path inside feature extractors. But in that case, the computed results are not "cached" and have to be rerun each time.&#x20;

The ideal in such cases will be to run the computation on the read path on demand but then to cache it for future invocations. And this is a common requirement while dealing with external API based data. This is what Fennel's "on demand" data comes in! Let's look at an example real quick:

```python
@dataset
class UserCreditScore:
    uid: int = field(key=True)
    credit_score: int
    
    @on_demand(expires_after='7d')
    def pull_from_api(ts: Series[datetime], uids: Series[uid]) -> DataFrame:
        user_list = uids.tolist()
        resp = requests.get(
            API_ENDPOINT_URL, json={"users": user_list}
        )
        ....
        return df, found
```

In this example, a function decorated with the decorator `on_demand` was added to the dataset. In broad strokes, on the online read path, when a lookup is issued, if the data doesn't exist in the dataset, the `on_demand` function is invoked with the missing keys. The function is free to run any code and the output of the function is stored in the dataset (as cache for future invocations) and returned back as the result of the lookup call.

The `on_demand` function takes an argument `expires_after` - this specifies the duration for which the data obtained from on demand function can be used before invoking the function with the same key again.&#x20;

### Signature

The signature of `on_demand` function is same as that of `lookup` but with just one difference - it's not possible to specify the subset of fields to be read. On demand must return all the fields (after all they need to be stored in the dataset)

```python
def <function_name>(
    cls, 
    ts: Series[datetime],  
    <ds_key1>: Series[<key1_dtype], 
    <ds_key2>: Series[<key2_dtype],
    ..., 
    <ds_keyn>: Series[<key1_dtype]
): -> Tuple[Dataframe, Series[bool]]
```

Couple of notes about the semantics of on demand datasets:

* On demand is only called on the online extraction path. It is never called during historical extraction (which can be looking up millions of things) or the write path.
* Once `on_demand` is called and some valid data is found, it is inserted in the dataset for future lookups. Any downstream datasets that depend on this dataset via pipelines will also get a chance to derive new diffs for themselves on the basis of pulled data.
