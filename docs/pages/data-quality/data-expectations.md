---
title: Data Expectations
order: 0
status: 'published'
---

# Data Expectations

Fennel's powerful [type system](/api-reference/data-types) lets you maintain data integrity by outright rejecting any data that doesn't meet the given types. However, sometimes there are situations when data expectations are more probabilistic in nature.&#x20;

As an example, you may have a field in dataset of type `Optional[str]` that denotes user city (can be None if user didn't provide their city). While this is nullable, in practice, we expect _most_ people to fill out their city. In other words, we don't want to reject Null values outright but still _track_ if fraction of null values is higher than what we expected.&#x20;

Fennel lets you do this by writing data expectations (and yes, this is based on Great Expectation - so you can reuse any expectations built by the community). Once expectations are specified, Fennel tracks the % of the rows that fail the expectation -- and can alert you about these failures. This can be a very powerful tool to catch a very large class of "runtime" quality issues.&#x20;

### Example

<pre snippet="data-quality/data-expectations#expectations" />

### Type Restrictions vs Expectations

[Type restrictions](/api-reference/data-types) and expectations may appear to be similar but solve very different purposes. Type Restrictions simply reject any row/data that doesn't satisfy the restriction - as a result, all data stored in Fennel datasets can be trusted to follow the type restriction rules.

Data expectations, on the other hand, don't reject the data - just passively track the frequency of expectation mismatch and alert if it is higher than some threshold. Type restrictions are a stronger check and should be preferred if no expectations to the restriction are allowed.&#x20;
