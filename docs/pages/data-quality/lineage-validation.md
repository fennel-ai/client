---
title: Lineage Validation
order: 1
status: 'published'
---

# Lineage Validation

Fennel has visibility into the full lineage of dataflow graph. During `sync`
call, Fennel conducts various validations across the full lineage graph to make
sure that the dataflow graph is valid. Sync call succeeds if and only if
all these validations pass at the compile time itself. These checks prevent
data quality bugs of the following kind:


## Missing Dependencies

If any construct depends on something else (e.g. a pipeline takes another dataset
as an input), Fennel validates that all such dependencies are available. This means
that a construct can not be deleted until everything that depends on it has been
deleted first.

When a feature depends on an upstream pipeline (possibly via multiple dependency hops),
deletion of that pipeline can lead to values of that dataset becoming nulls. This changes
the distribution of feature values leading to incorrect model results. But such
scenarios can not occur with Fennel because this missing dependency will be caught at
"compile time" itself


## Typing Mismatch

Fennel matches data types across dependencies to detect invalid relationships.
For instance, if a dataset is supposed to have a field of certain type but the pipeline that produces
the dataset doesn't produce that field/type, the error will be caught during `sync`
only without ever going into the runtime phase.


## Circular Dependencies

Fennel is also able to detect circular dependencies in dataflow during sync.
While these aren't that common in production, but when they do happen, can be
very hard to debug.
