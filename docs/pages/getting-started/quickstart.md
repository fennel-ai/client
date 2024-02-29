---
title: 'Quickstart'
order: 3
status: 'published'
---
# Quickstart

The following example tries to show how several concepts in Fennel come together to solve a problem.

### 0. Installation
We only need to install Fennel's Python client to run this example, so let's install that first:
```bash
pip install fennel-ai
```

And while we are at it, let's add all the imports that we will need in the
rest of the tutorial:

<pre snippet="getting-started/quickstart#imports"></pre>


### 1. Data Connectors

Fennel ships with data connectors that know how to talk to all common data
sources. The connectors can be defined in code or in Fennel console (not shown
here).
<pre snippet="getting-started/quickstart#connectors"></pre>

### 2. Datasets
Datasets are the tables that you want to use in your feature pipelines. These
are constantly kept fresh as new data arrives from connectors.
<pre snippet="getting-started/quickstart#datasets"></pre>

Fennel also lets you derive more datasets by defining pipelines that transform
data across different sources (e.g. s3, kafka, postgres etc.) in the same plane
of abstraction.  These pipelines are highly declarative, completely Python native,
realtime, versioned, are auto backfilled on declaration, and can be unit tested.
<pre snippet="getting-started/quickstart#pipelines"></pre>

### 3. Featuresets
Featuresets are containers for the features that you want to extract from your
datasets. Features, unlike datasets, have no state and are computed on the
"read path" (i.e. when you query for them) via arbitrary Python code. Features
are immutable to improve reliability.
<pre snippet="getting-started/quickstart#features"></pre>



### 4. Commit
Once datasets/featuresets have been written (or updated), you can commit those
definitions by instantiating a client and using it to talk to the server.

Since we are not working with a real server, here we use the MockClient to run
this example locally instead of a real client. Mock Client doesn't support data
connectors so we will manually log some data to simulate data flows.
<pre snippet="getting-started/quickstart#commit"></pre>

### 5. Query
This is the read path of Fennel. You can query for live features (i.e. features
using the latest value of all datasets) like this:
<pre snippet="getting-started/quickstart#query"></pre>

You can also query for historical values of features at arbitrary timestamps (
useful in creating training datasets) like this:

<pre snippet="getting-started/quickstart#historical"></pre>

Query requests can be made over REST API from any language/tool which makes it easy
to ship features to production servers.
