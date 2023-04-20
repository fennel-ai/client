---
title: 'Why Fennel?'
order: 1
status: 'draft'
---

# Why Fennel?

## The Problem

Feature engineering is hard -- data is spread over multitude of data sources like Postgres/MySQL, Kafka streams, Warehouse, S3 etc. Each of these have their own performance & data freshness characteristics and their own tooling to work with them.&#x20;

But features need to be written over all this data, plumbed in complex ways, served online with low latency, and maintained/developed by a team of people. Features also evolve, get deprecated, and run into data quality issues. All of it gets significantly harder when even a small number of realtime features are needed.

Here are some of the top problems that teams run into when trying to ship ML features in production:

| Authoring                                                                       | Governance                                            | Operations                             | Realtimeliness                                                         |
| ------------------------------------------------------------------------------- | ----------------------------------------------------- | -------------------------------------- | ---------------------------------------------------------------------- |
| Preferable to write Python over DSL or Spark/Flink pipelines                    | Hard to keep data & feature quality in check          | Ops overhead of lots of moving pieces  | Updating features in seconds with fresh data                           |
| Hard to test feature pipelines and do CI/CD                                     | Lineage tracking for privacy                          | Very high cloud costs                  | Low latency (millisecond) level serving                                |
| Hard to write features over data from multiple streaming and batch data sources | Tracking and fixing older or deprecated data/features | Security best practices for compliance | Keeping online served features & offline features for training in sync |

## Fennel's Approach

This is where Fennel comes in. Fennel is a modern feature engineering platform that simplifies the full life cycle of feature engineering - from authoring/updating, computation, serving, and monitoring. Fennel obsesses over the following, so you don't have to:

### Experience

* **Authoring** - features & pipeline authoring in Python using Pandas and other familiar libraries, instead of custom DSLs or YAML configs which are "sufficient" until they are not.
* **Powerful join capabilities** - do powerful joins (even on streaming data!) so you don't need to denormalize/enrich your data every time you add new features
* **Data Connectors** - pre-built data connectors to ingest data from batch and streaming sources like Postgres, S3, Snowflake, BigQuery, Kafka, and more.

### Power

* **Realtime** - support real-time features with minimum lag between the time an event is ingested and when feature values are updated as a result.
* **Blazing Fast** – support high-throughput low-latency reads (p99 of single digit millisecond)
* **Horizontally scalable** - horizontally scale to billions of events and feature reads per day

### Quality

* **Best engineering practices** - native support for unit & integration testing even for complex pipelines and features and CI/CD
* **Focus on correctness** - all code assets are versioned and/or immutable to prevent accidental changes, everything is strongly typed despite being Python native
* **Data quality checks \[coming soon]** - inbuilt support for specifying/tracking data expectations and drift monitoring

### Operations

* **Zero dependency install** - installs inside your VPC in minutes with zero dependencies, no need to bring your own Spark, Kafka, Warehouse, Redis etc. &#x20;
* **Fully managed ops** - 99.99% uptime SLA without thinking about machines, uptimes, autoscaling etc.&#x20;
* **Minimal Cloud Costs** - keep cloud costs as low as possible and pass all savings (without margin) to the customers
