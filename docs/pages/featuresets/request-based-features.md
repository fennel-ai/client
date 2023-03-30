---
title: Request Based Features
order: 3
status: 'published'
---

# Request Based Features

Many ML features in realtime applications like recommendations or fraud detection depend on the context of the user request. Such features and their dependencies are very naturally modeled in Fennel. Let's look at one good way of doing this:

```python
@featureset
class SearchRequest:
    uid: int = feature(id=1)
    time: datetime = feature(id=2)
    ip: str = feature(id=3)
    device_type: str = feature(id=4)
    query: str = feature(id=5)

@featureset
class UserFeatures:
    uid: int = feature(id=1)
    ...
    ctr_by_device_type: float = feature(id=17)
    ..
    
    @extractor
    @inputs(SearchRequest.device_type)
    fn f(cls, ts: pd.Series, devices: pd.Series): 
        for device in devices:
            ...

```

In this example, we defined a featureset called `SearchRequest` that contains all the properties of the request that are relevant for a feature. Features of other featuresets can now depend on `SearchRequest` features - in this example some features of `UserFeatures` are depending on `SearchRequest.device_type.` This way, as long as `SearchRequest` features are passed as input to extraction process, all such other features can be naturally computed.
