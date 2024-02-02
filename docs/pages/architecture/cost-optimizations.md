---
title: 'Cost Optimizations'
order: 3
status: 'published'
---

# Cost Optimizations

Production feature engineering systems deal with lots of data at low latencies
and hence can be very costly in terms of cloud infrastructure costs. Fennel's
built ground up to keep cloud costs low for any workload by squeezing as much 
out of cloud hardware as possible. 


## Incremental Ingestion & Processing

Feature engineering often involves aggregation of data over many days, weeks or 
even years. Batch processing paradigms ends up re-processing the same data many
times over (say once a day for 60 days for 60 day aggregations) - this is obviously
costly.

Fennel avoid this wastage by doing ONLY incremental ingestion and processing - this
way, even if the cost per unit of computation is higher for streaming, Fennel is 
able to net reduce cost by simply doing lot less computation. 

To accomplish this, Fennel uses in-house-built incremental data connectors to 
all data sources. Once data has been ignested, computation is done incrementally via 
Fennel's in-house streaming system written in Rust (see more below).

As a result, even "batch computation" ends up being cheaper via Fennel's incremental
approach.


## In-house Rust Streaming Engine

Fennel is powered by an-house Rust based streaming engine with similar goals as 
Flink / Spark Streaming but has a different architecture. Here are some noteworthy
differences:

- It is natively aware of CDC and translates user's pipelines to work across
  insert/delete/update "deltas".
- Event time is a first class object so much so that it's impossible to have a 
  dataset without the event time. As a result, out of order handling, watermarking,
  window aggregations, temporal joins etc are all handled by the framework itself.
- It doesn't buffer data in memory for windowing and instead relies heavily on 
  fast SSDs. As a result, it's able to support very long "out of order" timeframes
  while keeping a low memory footprint
- Each job/task has its own separate checkpointing (unlike Flink's global
  checkpointing). As a result, changes to job topology don't stop the whole world.
- No sync communication - all communication is async - either in-memory via 
  channels or via Kafka. As a result, shuffles, which typically take a lot of RAM
  in Flink/Spark, have tiny memory footprints.
- Built in Rust, no JVM, which is another reason why it has low memory footprint.

It may someday be open sourced as a standalone alternative to Spark/Flink but
for the time being, our focus is on making it really good for the workloads that
appear in the feature engineering world.


## K/V Serving Out of SSDs (RocksDB)

K/V stores are an important part of feature serving systems. Due to large throughput
needs, typically data is stored in an in-memory K/V store like Redis or a managed
disk based K/V store (like Dynamo or BigTable). Both of these options get very
costly at scale (though for different reasons).

Fennel does all K/V serving out of RocksDB instances on local SSDs. This both 
keeps the data on disk (so low memory overhead) and also avoid 2-3x margin by
cloud vendors. 

This required us to build machinery to manage shards & replicas of
RocksDB instances.


## Long tail of optimizations


In addition to factors mentioned above, there is a very long tail of cost 
optimizations across the system that work together to keep the costs low. Here 
is a non-exhaustive list of such optimizations:

* Keeping services stateless whenever possible so that spot instances could 
  power them
* Using minimal managed services provided by cloud vendors and instead running 
 the open source versions on our own on top of just the EC2 (this avoids 2-3x 
 markup charged by cloud vendors)
* Using efficient Rust to power all services to reduce CPU demands
* Using AWS Graviton processor based instances which offer better price/performance ratio
* Choosing the right instance family and size for each sub-system
* Auto scaling up/down various clusters depending on the workload (e.g. reduce 
  costs at night)
* Tightly encoding data in binary formats (e.g. using variable length integers, 
 etc.) in all storage engines to reduce storage and network bandwidth costs
* Adding compression (say at disk block level) in data storage systems
* Data tiering - preferring to keeping data in S3 vs instance store vs RAM 
  whenever possible
* Avoiding network costs by preferably talking to data sources in the same AZ
* ...and lot more
