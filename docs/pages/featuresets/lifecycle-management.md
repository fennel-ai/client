---
title: Lifecycle Management
order: 5
status: 'published'
---

# Lifecycle Management

ML Features have a complex lifecycle. Here are some common scenarios:

* Features are added for experimentation and need to be removed if the experiment doesn't show metric wins
* A bug is discovered in a feature that is already live in a model. Sometimes it is desirable to patch the feature live (which may mean that model is now getting feature distribution for which it wasn't trained) and sometimes it is desirable to create a separate new feature and use it in a new training run before deprecating the older one.&#x20;
* Sometimes a feature depends on a data pipeline. And data pipeline is removed while the feature is still live in a production model. That leads to feature getting invalid values leading to silent model degradation

Fennel aims to handle these and more cases in the "right way" and preventing users from making common bugs.

### Immutability, Versioning, & Evolution

Individual features are immutable once created - i.e. their name, the `id`, the `type`, and their extractor code can never change. This is done to prevent a host of bugs and issues when feature code changes over time leading to models getting features that they were not trained on.&#x20;

While individual features are immutable, featuresets can evolve over time by adding/removing features - this is done by simply adding new features with higher `id` and/or deprecating/deleting older features. This behavior is similar to adding/deprecating tags in protobufs. The way to "fix" a feature is to add a new feature with different ID and optionally mark the previous versoin as deprecated/deleted.&#x20;

### Dependency Validation

Fennel explicitly tracks the lineage graph across ALL featuresets and datasets - as a result, Fennel is able to identify cases when something (say a feature) recursively depends on something (say a dataset) that is now being deleted. Fennel checks the integrity of dependency graph at the `sync` time and doesn't let the system get in such states.&#x20;

As a result, the only way to delete something (a dataset, a feature, whole featureset) is to first delete all other things that recursively depend on this.

### Metaflags

Similar to dataset metaflags, featuresets, features, and extractors can also be annotated with [metaflags](/governance/metaflags) to manage their life cycle. Here is an example:

<pre snippet="featuresets/lifecycle#featureset_metaflags" />

Here is what each metaflag means in the context of features:

1. **Owner** - the email of the person/entity who is responsible for maintaining health of the featureset/feature. Fennel requires every featureset to have an owner to encourage healthy code maintenance.&#x20;
2. **Deprecated** - a feature or featureset can be marked as deprecated to communicate the intent of removing it at some point of time in the future. This serves as documentation and deters new dependencies on the deprecated feature. In the near term, it will be possible to configure Fennel to send periodic emails to any downstream users of the deprecated feature to nudge them to migrate to alternatives.&#x20;
3. **Deleted** -  when a feature is marked as deleted it will no longer be available in the system. Using deleted features elsewhere in the system will fail at the sync time itself.
4. **Description** - optional description to provide to any feature - most commonly used for documentation, not only in code but also in metadata APIs, in Fennel console etc.
5. **Tags** - list of arbitrary string tags associated with each feature.
