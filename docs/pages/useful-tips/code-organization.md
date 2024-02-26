---
title: 'Code Organization'
order: 0
status: 'published'
---

# Code Organization

Fennel code can be flexibly organized in a variety of ways - it's plain Python
after all. That said, here are some tips that have been seen to work well across
various teams using Fennel - feel free to use or adapt these to your needs.

You can also checkout end-to-end example projects [here](https://github.com/fennel-ai/client/tree/main/examples) for inspiration.

## Module Organization
The simplest of the projects may have only four separate modules - one each for 
datasets, featuresets, sync script, and tests.
```bash
fennel-project/
    | datasets.py
    | featuresets.py
    | sync.py
    | test.py
```
With this structure, as the names imply, all source & dataset definitions go 
in `datasets.py`, all featureset definitions go in `featuresets.py`, tests go in 
`test.py` and the script to instantiate Fennel client and make the sync request 
goes under the `if __name__ == 'main'` block in `sync.py`.

As the complexity grows, these modules may need to be factorized. One natural 
approach is to convert `datasets` and `featuresets` modules to be directories 
and organize sub-modules under them based on the domain. Since the sources 
(e.g. s3 credentials) are needed for each dataset sub-module, they can be further
factorized in their own module. Tests could be moved to a top-level `/tests`
or `/tests` under both datasets/featuresets or unbundled as `test_x.py` files -
really upto your personal taste.

Overall, the structure could look something like this:

```bash
fennel-project/
    | sources.py
    | datasets/
        | __init__.py
        | user.py
        | product.py
        | ...
    | featuresets/
        | __init__.py
        | user.py
        | click_history.py
        | ...
    | sync.py
    | tests / 
        | test_user.py
        | ...
```

From this point onwards, the structure can scale and grow like any other Python
project.

## Unit Tests With Data Files
Fennel has a [strong typing system](/api-reference/data-types) which makes it 
easier to detect & catch data quality issues. However, this also adds some
overhead in getting the type of each dataset field right.

A common pattern is to checkin a sample data file for each sourced dataset and 
[log](/api-reference/client/log) the contents of the file into the dataset as 
part of a unit test. This ensures that the unit test passes if and only if the
dataset field types all match the contents of the sample file.

With this, the directory structure may look like this:
```bash highlight="3-6"
fennel-project/
    | sources.py
    | data/ 
        | user_signups.csv
        | transactions.parquet
        | ...
    | datasets/
        | ...
    | featuresets/
        | ...
    | sync.py
    | tests/ 
        | ...

```
Checkout this example [data](https://github.com/fennel-ai/client/tree/main/examples/fraud/data) 
directory and this [test directory](https://github.com/fennel-ai/client/tree/main/examples/fraud/tests) 
to see tests that use this pattern.

## Organizing Featuresets

Fennel featuresets are just a collection of some features. It's entirely possible 
to create a single featureset with all the features in it. It's also possible
to create thousands of featuresets each with a single feature. There is no right
or wrong way of grouping features. 

That said, here are some useful ways to organize your featuresets:

- **Featureset per entity**: featuresets could be mapped 1:1 to entities,
  for instance, by having one featureset for `User` features, a separate 
  featureset for all `Product` features, a third featureset for `UserSeller` 
  features that involve user's interaction with the seller in the past and so on.
- **Featureset per domain**: featureset could be mapped to business domains, 
  for instance in a bank, having one featureset for credit card balance
  features, one for checkin account features, one for mortgage account and so on.
- **Featureset per request type**: almost always, special featuresets are needed
  that map 1:1 with the information contained in the inference requests - for instance
  such a featureset will have one feature for uid, one for time of request, one
  for IP from which user is logging in and so on. These featuresets have no extractors
  so all these features need to be provided as inputs.