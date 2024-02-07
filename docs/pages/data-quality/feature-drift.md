---
title: Feature Drift
order: 6
status: 'published'
---

# Feature Drift

Fennel monitors the live probability distribution of all extracted
features. It does so by measuring the mean and the standard deviation of the
extracted feature values.

Changes in the probability distribution of a feature are usually indicative of 
one of the three scenarios:

1. Organic drift over time
2. Major changes to feature definitions
3. Newly introduced bugs

Either way, it's useful to be able to track the feature distribution. These 
distributions can be seen on [Fennel console](/development/console).

![Diagram](/assets/feature_drift.png)

:::info
Feature distributions are tracked only for features having a [type](/api-reference/data-types/core-types) 
of either `int` or `float`.
:::

