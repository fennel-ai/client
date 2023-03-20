---
title: End to End Extraction
order: 1
status: 'published'
---

# End to End Extraction

Once one or more featuresets have been defined, you can ask Fennel to extract some features (possibly belonging to many different featuresets). Let's say we have the following featuresets:

```python
@featureset
class User:
    id: int = feature(id=1)
    age: float = feature(id=2)
    ...


@featureset
class UserPost:
    uid: int = feature(id=1)
    pid: int = feature(id=2)
    score: float = feature(id=3)
    affinity: float = feature(id=4)
    ...

@featureset
class Request:
    ip: str = feature(id=1)
    ...

                                                        
```

A request can be made to the Fennel servers by running the following code:

<pre snippet="featuresets/e2e_extraction#e2e_extraction" />

A request is made to the server in the above example. Lines 2-7 specify the list of features that need to be extracted - note that this contains features across different featuresets. Between lines 8-13, a list of known features are provided and the values of these features are provided in lines 14-19. Fennel will start with the output features, find their extractors, find the inputs of those extractors and continue that process recursively until it can find a path from the given input features to all the desired output features. If no such path can be found, an error is thrown.&#x20;

A few notes here:

* Both the output feature list and the input feature list can span multiple featuresets
* Multiple 'rows' can be provided in the `input_dataframe` i.e. features for all these data points are extracted together. This is a common requirement for ranking use cases where multiple candidates need to be ranked against each other.
* Here we provided both `User.id` and `UserPost.uid` as inputs. If the semantics are such that they refer to the same user, it's possible to write an extractor, say in `UserPost` featureset depending on `User.id` that just returns the input back. If that was done, you could get away by providing only `User.id`. More generally, this way, featuresets can be linked such that final extraction calls only require primitive IDs and maybe some context from the request.
