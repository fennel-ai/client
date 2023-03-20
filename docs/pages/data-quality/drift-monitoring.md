---
title: Drift Monitoring
order: 2
status: 'published'
---

# Drift Monitoring

The distribution of ML features can change organically over time despite the system being correct. For instance, when the weather shifts from winter to summer, the demand for ACs rises sharply. As a result, the distribution of any features that capture demand of products is going to change too. This is not a problem in itself unless models haven't been refreshed in a while - in that case, the trained model's performance may degrade due to this drift in feature values.&#x20;

Fennel will track feature distributions and give you alerts when they skew meaningfully. This is currently work in progress and is expected to land in Q2 2023.

