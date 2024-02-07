---
title: New Relic
order: 3
status: 'published'
---

# Integrating Fennel with New Relic

Fennel supports exporting metrics to New Relic through Prometheus Remote Write 
Integration. Here is how to pipe Fennel metrics into your New Relic:

1. Go to the administration page in the user or the organization account
![Diagram](/assets/newrelic_1.png)
2. Create a new API Key of type `Ingest - License` on New Relic
![Diagram](/assets/newrelic_0.png)
3. Share the API key with the Fennel team along with an appropriate data source name 
   (e.g. fennel_dev_metrics_source, fennel_staging_metrics_source etc) of choice.
   This data source name will help you identify the source of a particular metric 
   in new relic by using using `instrumentation.source`.

