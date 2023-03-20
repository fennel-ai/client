---
title: Reading Datasets
order: 2
status: 'published'
---

# Reading Datasets

More often than not, features need to be built upon some data precomputed on the write path in a Dataset. Extractors need to be able to read datasets. As explained in the dataset lookup section, lookups are the bridge between datasets and featuresets. Here is an example of how that looks:

<pre snippet="featuresets/reading_datasets#featuresets_reading_datasets" />


Here, in line 18, the extractor is able to read the names of the users from the dataset. But since this creates a dependency between the featureset and the dataset, Fennel requires the dependency to be declared explicitly. In line 16, the extractor specifies that it is going to do lookups on `User` dataset. If an extractor does lookups on many datasets, all their names can be passed as a list e.g. `@depends_on(User, Post, SomeDataset)`
