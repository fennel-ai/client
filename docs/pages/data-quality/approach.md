---
title: Approach
order: 0
status: 'published'
---

# Approach

Maintaining correctness of data and features is a top three [design goal](/) for
Fennel. 

While Fennel has several diagnostic and monitoring levers too, unlike many other
systems out there, Fennel's approach leans heavily on preventive measures
that prevent failures from happening in the first places.

Here are some of the key ideas that help prevent/diagnose data quality issues:

| Type       | Method                          | Details                                       |
| ---------- | ------------------------------- | --------------------------------------------- |
| Preventive | Strong Typing                   | [Link](/data-quality/strong-typing)           |
| Preventive | Immutability & Versioning       | [Link](/data-quality/immutability-versioning) |
| Preventive | Unit Testing                    | [Link](/development/unit-testing)             |
| Preventive | Compile time lineage validation | [Link](/data-quality/lineage-validation)      |
| Preventive | Structured metadata & ownership | [Link](/data-quality/metaflags)               |
| Diagnostic | Data Expectations               | [Link](/data-quality/data-expectations)       |
| Diagnostic | Feature Drift Detection         | [Link](/data-quality/feature-drift)           |
          

Each of these methods is already powerful on their own. And their preventive/diagnostic power
further amplifies when applied together. 