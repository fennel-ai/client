---
title: Composite Features
order: 4
status: 'published'
---

# Composite Features

In many cases, it's important to write features about a composite of two or more objects. For instance, in content recommendation, there are some features which may only be defined for (user, content) pair e.g. affinity of user with content creator. Even these complex features can be naturally modeled using Fennel featuresets. Here is a relatively complex example:

```python
@featureset
class User:
    id: int = feature(id=1)
    name: str = feature(id=2)
    ..

@featureset
class Post:
    id: int = feature(id=1)
    creator_uid: int = feature(id=2)    
    ...
    
    @extractor
    def creator(cls, ts: pd.Series[datetime], pids: pd.Series[id]) -> Series[creator_uid]:
        <some code here>
        ...

@featureset
class UserCreator:
    # describes features for (uid, creator_uid) pairs
    viewer: int = feature(id=1)
    creator: int = feature(id=2)
    affinity: float = feature(id=3)
    ...

    @extractor
    def affinity_fn(cls, ts: pd.Series[datetime], viewers: pd.Series[viewer], 
                    creators: pd.Series[creator]) -> Series[affinity]:
        <some code here>
        ...

@featureset
class UserPost:
    # describes features for (uid, pid) pairs
    uid: int = feature(id=1)
    pid: int = feature(id=2)
    viewer_author_affinity = feature(id=3)
    ...
    
    @extractor
    def fn(cls, ts: pd.Series[datetime], uids: pd.Series[uid], pids: pd.Series[pid]):
        creators = Post.creator(ts, pids)
        return UserCreator.affinity_fn(ts, uids, creators)
            
```

A lot is happening here. In addition to featureset for `User`, and `Post`, this example also has a couple of composite featuresets -- `UserCreator` to capture features for (uid, creator) pairs and `UserPost` for capturing features of (uid, post) pairs.&#x20;

Further, extractors can depend on extractors of other featuresets - here is line 42, the extractor first uses an extractor of `Post` featureset to get creators for the posts and then users an extractor of `UserCreator` to get the affinity between those users and creators.&#x20;

This way, it's possible to build very complex features by reusing machinery built for other existing features. And Fennel is smart enough to figure out the best way to resolve dependencies across featuresets (or throw an error if they can't be satisfied).&#x20;
