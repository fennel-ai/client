---
title: Metrics
order: 2
status: 'published'
---

# Metrics Export
For monitoring/alerting, Fennel exposes all relevant metrics behind an 
OpenMetrics compatible endpoint. You can point [Grafana](/observability/grafana), 
[New Relic](/observability/newrelic), [Datadog](/observability/datadog) or any 
other metric system that speaks the OpenMetrics protocol towards this endpoint. 
Once your metric system is connected to Fennel endpoint, you can seamlessly 
use your existing monitoring/alerting stack.

The following metrics are exposed by Fennel:
## Write path metrics
<ol>
<li>
<details>
   <summary><b> `fennel_source_dataset_metrics` </b></summary>

   Reports the rate & the health of data ingestion from sources into datasets

   Type: Gauge

   *Labels*:
    - `source_name` - name of the source
    - `dataset_name` - name of the dataset
    - `metric` - metric to report. Possible value are `backlog` & `event_timestamp_seconds`. 
        `backlog` reports the number of events ingested by the source but yet to be applied to the dataset.
        `event_timestamp_seconds` reports the event timestamp of the last message processed (this is sampled and hence approximate).
   </details>
</li>

<li>
<details>
   <summary><b> `fennel_source_dataset_counter` </b></summary>

    Reports the count of schema errors encountered by the data source

    Type: Counter

   *Labels*:

    - `source_name` - name of the source
    - `dataset_name` - name of the dataset
    - `format` - format of the data being processed
    - `metric` - metric to report. Possible value are `msgs_processed` & `schema_error`. 
        `msgs_processed` reports the number of messages processed by the source dataset so far.
        `schema_error` reports the number of messages which failed due to schema mismatch related errors.
   </details>
</li>



<li>
<details>
   <summary><b> `fennel_pipeline_metrics` </b></summary>

    Reports the volume and the health of data flowing through the pipeline into the dataset

    Type: Gauge
    

   *Labels*:
    - `pipeline_name` - the name of the pipeline
    - `dataset_name` - the name of the dataset
    - `metric` - metric to report. Possible values are  `backlog`, `event_timestamp_seconds`, `error`.
     `backlog` reports the number of events in the input datasets that haven't been processed by the pipeline.
     `event_timestamp_seconds` reports the event timestamp of the last message processed (this is sampled and hence approximate).
     `error` is the number of messages for which the pipeline encountered an error.
   </details>
</li>

<li>
<details>
   <summary><b> `fennel_expectation_status_counter` </b></summary>

   Reports the number of rows in the dataset that passed/failed the given
   data expectation

   Type: Counter

   *Labels*:
   - `dataset` - the name of the dataset whose data is monitored
   - `expectation_name` – the name of the specific expectation
   - `status` - whether expectation passed or not. Valid values are `success` or `failure`
   </details>
</li>

<li>
<details>
   <summary><b> `fennel_source_watermark_discarded_rows` </b></summary>

   Reports the number of rows discarded at the source dataset due to watermarking i.e. the rows are older than the 
   `disorder` set on the source.

   Type: Counter

   *Labels*:
   - `source_name` - the name of the source
   - `dataset_name` – the name of the dataset
   </details>
</li>

<li>
<details>
   <summary><b> `fennel_source_watermark_timestamp` </b></summary>

   Reports the current watermark timestamp of the source dataset. Any event older than this timestamp is discarded 
   and reported in `fennel_source_watermark_discarded_rows` metric.

   Type: Gauge

   *Labels*:
   - `source_name` - the name of the source
   - `dataset_name` – the name of the dataset
   </details>
</li>
</ol>


## Read path metrics
<ol>

<li>
<details>
   <summary><b> `fennel_request_counter` </b></summary>

   Reports the number of API calls to Fennel broken by the endpoint.

   Type: Counter

   *Labels*:
    - `target` - the endpoint invoked. Valid values are `sync`, `extract_features`, `log`, `extract_historical_features`.
    - `code` - the status code corresponding to the request. Valid values are valid HTTP codes.
   </details>
</li>

<li>
<details>
   <summary><b> `fennel_extract_features_counter` </b></summary>

   Reports the number of times a given feature has been extracted.

   Type: Counter

   *Labels*:
    - `featureset` - the name of the featureset in which the feature to be extracted is defined
    - `feature` - the name of the feature to be extracted
    - `workflow` - the name of the workflow set in the feature extraction call. Note that it defaults to `default` when not set.
    - `target` - either `extract_features` or `extract_historical_features`
   </details>
</li>

<li>
<details>
   <summary><b> `fennel_extract_features_error_counter` </b></summary>

   Reports the number of errors in feature extraction calls by workflow

   Type: Counter

   *Labels*:
    - `workflow` - the name of the workflow set in the feature extraction call. Note that it defaults to `default` when not set.
    - `target` - either `extract_features` or `extract_historical_features`.
    - `code` - the status code corresponding to the request. Valid values are valid HTTP codes.
   </details>
</li>

<li>
<details>
   <summary><b> `fennel_request_latency_seconds` </b></summary>

   Reports the latencies of the API calls made to Fennel.

   Type: Histogram

   *Labels*:
    - `target` - valid values are `extract_features`,`extract_historical_features`, `sync`, and `log`.
   </details>
</li>

<li>
<details>
   <summary><b> `extract_features_latency_seconds` </b></summary>

   Reports the latencies of feature extraction calls in particular.

   Type: Histogram

   *Labels*:
    - `workflow` - the name of the workflow set in the feature extraction call. Note that it defaults to `default` when not set.
    - `target` - either `extract_features` or `extract_historical_features`.
   </details>
</li>
</ol>

