---
title: 'Performance Tuning'
order: 2
status: 'published'
---

# Performance Tuning


## Derived Extractors

For certain common extractor patterns, Fennel provides the ability to derive these extractors as part of their
feature definitions. These derived extractors carry metadata that allow the fennel backend to significantly improve performance: for these extractors, Fennel avoids calling into the Python runtime generally needed to execute extractor logic.  The following extractor types are supported:

1. Dataset lookup extractors. These extractors perform a lookup on a single field of a 
dataset, potentially supply a default value for missing rows, and assign the 
output to a single feature. Here's an example of a manually written extractor of this form:
<pre snippet="featuresets/reading_datasets#featuresets_reading_datasets"></pre>

2. Aliases. These extractors unidirectionally map an input feature to an output feature. 

### Examples
These extractors are derived by the `feature.extract()` function. Here is an example:
<pre snippet="featuresets/reading_datasets#derived_extractors"></pre>

In this example, `UserFeaturesDerived.uid` is an alias to `Request.user_id`. Aliasing is 
specified via the `feature` kwarg. `UserFeaturesDerived.name` specifies a lookup extractor,
with the same functionality as the extractor `func` defined above. 
The lookup extractor uses the following arguments:
* `field` - The dataset field to do a lookup on
* `default` - An optional default value for rows not found  
* `provider` - A featureset that provides the values matching the keys of the dataset 
      to look up. The input feature name must match the field name. If not 
      provided, as in the above example, then the current featureset is assumed
      to be the provider. If the input feature that the extractor needs does not
      match the name of the field, an `alias` extractor can be defined, as is the
      case with `UserFeaturesDerived.uid` in the above example.

Here is an example where the input provider is a different featureset:
<pre snippet="featuresets/reading_datasets#derived_extractor_with_provider"></pre>