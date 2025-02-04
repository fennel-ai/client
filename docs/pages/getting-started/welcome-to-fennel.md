---
title: 'Welcome to Fennel'
order: 0
status: 'published'
slug: '/'
---

# Welcome to Fennel

Fennel is a modern compute engine for building both batch & realtime ML pipelines 
inspired by the ethos of PyTorch and is architected in the service of the 
following three design goals:

## Fennel's Three Design Goals
1. **Easy to install, learn & use** - be approachable to data 
   science / ML teams and to make them fully self-dependent for shipping
   production-grade ML pipelines.
2. **Promote development and governance related best practices** - production ML 
   systems often get plagued by variety of problems like data/feature quality 
   issues. Prevention & detection of such issues should be a first-class citizen, 
   not an afterthought. This requires both embedded best practices and tooling
   like lineage for governance. (Read [more](/data-quality/approach))
3. **Keep cloud costs low** - feature engineering often requires dealing with large
   volumes of data and gets very costly. For any workload, Fennel aims
   to be at least as cost-effective as a hand-rolled system while also being 
   more powerful & easier to use. (Read [more](/architecture/cost-optimizations))

## Use Cases

Fennel can be used to build all sorts of ML pipelines - both predictive AI 
pipelines on structured data (e.g. feature engineering which is one of the more 
common workloads) as well as generative AI pipelines on unstructured data 
(e.g. text processing via LLMs). 

Here are some of the most common use cases where Fennel is used in production:

- **Fraud Detection** - relevant for fintech or anywhere money exchange is involved
- **Risk Engineering** - identifying fake users, spam, bot rings and lot more
- **Credit Underwriting** - use cases dealing with credit risk like credit cards, 
  buy-now-pay-later, loan underwriting etc. 
- **Insurance Underwriting** - assessing the probability of filing a claim, expected
  damages etc.
- **Recommendations / Personalization** - relevant for most marketplaces, e-commerce products, 
  content feeds, notifications, email marketing campaigns etc. 
- **RAG for customer support** - building a chat bot to answer customer queries
  utilizing a large corpus of documents (e.g. FAQs, product manuals, etc.)
- **Search Ranking** - ordering available documents in response to a query
  based on relevance, personalization, or custom preferences
- **Marketing Personalization** - choosing the right promotions/discounts to offer,
  the right time to send a campaign email, the right products to include in the email etc.
- **User Segmentation** - segmenting users based on their behavior, preferences, 
  demographics etc.
- **User Engagement/Retention** - predicting churn, identifying up sell opportunities, etc.

.. and lot more!

Fennel is being actively developed to extend this list with a special focus on 
generative AI use cases - please reach out to us via (support [at] fennel.ai)
if you have any ideas or feedback.


## Getting Started With Fennel

[Start](/getting-started/quickstart) here if you want to directly dive deep into
 an end-to-end example.&#x20;

Or if you are not in a hurry, read about the main [concepts](/concepts/introduction) 
first followed by some more details about Fennel's [architecture](/architecture/overview).

We, the team behind Fennel, have thoroughly enjoyed building Fennel and hope 
learning and using Fennel brings as much delight to you as well!
