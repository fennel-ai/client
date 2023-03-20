---
title: Metaflags
order: 1
status: 'published'
---

# Metaflags

Features and datasets are not static in the real world and have their own life cycle. Metaflags is a mechanism to annotate and manage the lifecycle of Fennel objects.&#x20;

Here are a few scenarios which are pretty common in practice where Metaflags help:

* A bug is discovered in a feature, resulting in it being rewritten. This now requires all downstream features to also be updated.&#x20;
* Ownership of a dataset needs to be tracked so that if it is having data quality issues, the problem can be routed to an appropriate person to investigate.
* Features and data need to be documented so that their users can easily understand what they are doing.&#x20;
* Due to compliance reasons, all features that depend on PII data either directly or through a long list of upstream dependencies need to be audited - but for that, first all such features need to be identified.&#x20;

Let's look at an example:

```python
@meta(owner='nikhil@xyz.ai', tags=['PII', 'hackathon'])
@dataset
class User:
    uid: int = field(key=True)
    height: float = field().meta(description='in inches')
    weight: float = field().meta(description='in lbs')
    at: datetime

@meta(owner='feed-team@xyz.ai')
@featureset
class UserFeatures:
    uid: int = feature(id=1)
    zip: str = feature(id=2).meta(tags=['PII'])
    bmi: float = feature(id=3).meta(owner='alan@xyz.ai')        
    bmr: float = feature(id=4).meta(deperecated=True)
    ..
    
    @meta(description='based on algorithm specified here: bit.ly/xyy123')
    @extractor
    def some_fn(...):
        ...
```

Fennel currently supports 5 metaflags:

1. **owner** - email address of the owner of the object. The ownership flows down transitively. For instance, the owner of a featureset becomes the default owner of all the features unless it is explicitly overwritten by specifying an owner for that feature.&#x20;
2. **description** - description of the object, used solely for documentation purposes.&#x20;
3. **tags** - list of arbitrary string tags associated with the object. Tags flow across the lineage graph and are additive. For instance, if a dataset is tagged with tag 'PII', all other objects that read from the dataset will inherit this tag. Fennel supports searching for objects with a given tag.&#x20;
4. **deleted** - whether the object is deleted or not. Sometimes it is desirable to delete the object but keep a marker tombstone in the codebase - that is where deleted should be used. For instance, maybe a feature is now deleted but its ID should not be reused again - it'd be a good idea to mark it as deleted and leave it like that forever (the code for its extractor can be removed)
5. **deprecated** - same as deleted but just marks the object as to be deprecated in the near future. If an object uses a deprecated object, the owner will get periodic reminders to modify their object to not dependon the deprecated object any more.&#x20;

:::info
Most Fennel constructs are immutable by construction. However, it's possible to change their metaflags even when the rest of the details can not be changed.
:::
