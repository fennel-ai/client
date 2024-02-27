---
title: Immutability & Versioning
order: 2
status: 'published'
---

# Immutability & Versioning

Most Fennel constructs (datasets, featuresets, extractors etc.) are immutable
and can not be changed once created (unless explicitly versioned). So downstream
users of any object can confidently rely on the object to be stable.

## Enforcement of immutability

Fennel tracks the state of all entities and during any `commit`, verifies that
the entities that were supposed to be immutable haven't changed. If any such 
change is detected, the whole commit operation fails.

This ensures that all definitions are immutable unless explicitly updated by a 
human by changing its version, at which point, the change is going via the code
review system and can be quality controlled appropriately.

## Preventive power of immutability

In the messy real world where pipelines & features are constantly evolving, this
simple guarantee prevents many bugs. Some examples:

* A model was trained using a certain definition of the feature but later the definition of
  that feature got changed. So now model is doing inference on a feature distribution
  that it hadn't seen before during training, leading to degraded performance.

* A dataset was populated using a certain definition of the pipeline but the pipeline
  code has changed since then. As a result, the dataset contains some rows from the
  previous version and some from the new one but it's all mixed up. This violates
  data semantics and changes distribution of ML features using the data.

