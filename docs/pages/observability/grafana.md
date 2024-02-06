---
title: Grafana
order: 2
status: 'published'
---

# Integrating Fennel With Grafana

You can easily plot [metrics exposed by Fennel](/observability/prometheus) on your own
Grafana dashboards by adding it as a Metric Data Source in Grafana. Here is how
to do it:

## Setting up Grafana
  1. Choose a name for the data source (e.g. `Fennel Prod`).
![Diagram](/assets/grafana_0.png)
  2. Add the Prometheus Server URL as `https://<your-cluster-url>.fennel.ai/prometheus/`
  3. Enable `Basic Auth`, set User as `username`. Contact Fennel support to
     get the password.
  4. Save the data source


## Dashboard Template

You can use the following Grafana Dashboard configuration for an out-of-the-box 
setup (and modify them subsequently as required). 

  1. Create a dashboard by using `Import` option under `New` button
![Diagram](/assets/grafana_1.png)
  2. Paste [this JSON file](https://gist.github.com/mohitreddy1996/2a226fcfacd30778ff826850bbd65020)
  3. Select the Prometheus data source name as selected above in the integration
![Diagram](/assets/grafana_2.png)