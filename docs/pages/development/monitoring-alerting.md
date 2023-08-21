---
title: Monitoring & Alerting
order: 3
status: 'published'
---

# Monitoring & Alerting

Fennel is inspired by the Unix philosophy of doing a few things
really well and seamlessly composing with other tools for the rest.

For monitoring/alerting, Fennel exposes all relevant metrics behind a Prometheus
endpoint. You can point Grafana, Datadog, or any other metric system that speaks
the Prometheus protocol towards this endpoint. Once your metric system is
connected to Fennel's Prometheus endpoint, you can seamlessly use your existing
monitoring/alerting stack.


## Incident Management & PagerDuty
Since Fennel is completely managed, you don't need to be on PagerDuty for incidents
affecting the Fennel system itself - Fennel engineers get paged for those.

However, if you wanted to set your own PagerDuty alerting for application metrics,
you can do so on your own on top of the metrics exposed behind the Prometheus
endpoint.


## Exposed Metrics

Some examples of metrics that are available behind this Prometheus endpoint:
- Count & latencies of all API calls
- Lag of pipelines and data sources
- Error counts, along both read and write paths
- Mean and variance of extracted feature values
- Results of expectations defined against datasets
