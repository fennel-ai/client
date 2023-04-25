---
title: Request Based Features
order: 0
status: 'published'
---

# Request Based Features

Many ML features in real world ML systems like recommendations or fraud detection
depend on the context of the user request. Such features and their dependencies
are very naturally modeled in Fennel. Let's look at one good way of doing this:

```python
@featureset
class SearchRequest:
    time: datetime = feature(id=1)
    ip: str = feature(id=2)
    device_type: str = feature(id=3)
    query: str = feature(id=4)

@featureset
class UserFeatures:
    uid: int = feature(id=1)
    ...
    ctr_by_device_type: float = feature(id=17)
    ..
    
    @extractor
    @inputs(SearchRequest.device_type)
    @outputs(ctr_by_device_type)
    def f(cls, ts: pd.Series, devices: pd.Series): 
        for device in devices:
            ...

```

In this example, we defined a featureset called `SearchRequest` that contains 
all the properties of the request that are relevant for a feature. This featureset
itself has no extractors - Fennel doesn't know how to extract any of these features.

Features of other featuresets can now depend on `SearchRequest` features - in this 
example some features of `UserFeatures` are depending on `SearchRequest.device_type.` 
This way, as long as `SearchRequest` features are passed as input to extraction
process, all such other features can be naturally computed.

This makes it very easy to mix and match computation & data lookups based on some
request properties.
