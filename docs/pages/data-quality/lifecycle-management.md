---
title: Lifecycle Monitoring
order: 1
status: 'published'
---

# Lifecycle Management

When it comes to data quality, prevention is better than the cure. To that end, three powerful ideas are built into the Fennel's core architecture that together prevent a whole class of bugs/issues organically without any external monitoring:

* **Explicit Strong Typing** - all data has to be explicitly associated with types. Further, the type system is rather strong - e.g. nulls are't allowed unless the type is marked to be Optional. Fennel also supports much stronger [type restrictions](/api-reference/data-types) to express even more complex constraints (e.g. matching a regex)
* **Immutability** - most objects (datasets, featuresets, extractors etc.) are immutable and can not be changed once created. So downstream users of any object can confidently rely on the object to be stable.
* **Dependency Validation** - Fennel tracks the full dependency graph across all objects - datasets, featuresets, features, extractors etc. As a result, it is able to detect and prevent a whole class of common data/feature quality issues at "compile" time (i.e. when the `sync` call is made).&#x20;

Here are some common sources of bug and how these two best practices prevent them:



| Scenario                                                                                                                                                                      | How Fennel prevents it                                                                                                                     |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| A feature recursively depends on some data pipeline which is later removed leading to feature values becoming all NULLs                                                       | The change that deleted the dataset would have been rejected in `sync` - because a non-deleted feature depends on it                       |
| A feature recursively depends on some data but the data pipeline code changes, leading to wrong feature values                                                                | Pipelines are immutable and can not be modified                                                                                            |
| A feature F1 depends on another feature F2. F1 is part of a model but F2 is not. F2's owner modifies it since no model is using it, causing distribution of F1 to also change | Feature dependencies are tracked explicitly and mutations aren't allowed.                                                                  |
| A dataset contains a string column representing zip codes. But due to a bug upstream, non-zip code strings are added, which can break any pipelines depending on this dataset | Zip code can be expressed a specific type restriction using regex. Any data not confirming to it will not even be admitted to the dataset. |

