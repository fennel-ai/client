---
title: 'Cost Optimizations'
order: 3
status: 'published'
---

# Cost Optimizations

Production feature engineering systems deal with lots of data at low latencies
and hence can be very costly on cloud infrastructures. Fennel goes an extra
mile in trying to keep cloud costs by squeezing as much out of hardware as 
possible. This is accomplished by a long tail of cost optimizations -- many of
which are too low ROI for each team to invest in individually but become feasible
due to economies of scale enjoyed by Fennel.

Here is a non-exhaustive list of such optimizations:

* Keeping services stateless whenever possible so that spot instances could power them
* Serve features using RocksDB based disk/RAM hybrid instead of more costly options like DynamoDB or Redis (which keeps all data in RAM)
* Using minimal managed services provided by cloud vendors and instead running the open source versions on our own on top of just the EC2 (this avoid 2-3x markup charged by cloud vendors)
* Using efficient Rust to power all services to reduce CPU demands
* In particular, not relying on more general purpose streaming systems like spark or Flink but using an in-house Rust based system purpose built for feature engineering workloads with much lower overhead
* Using AWS graviton processor based instances which offer better price/performance ratio
* Auto scaling up/down various clusters depending on the workload (e.g. reduce costs at night)
* Tightly encoding data in binary formats (e.g. using variable length ints etc.) in all storage engines to reduce storage and network bandwidth costs
* Adding compression (say at disk block level) in data storage systems
* Data tiering - preferring to keeping data in S3 vs instance store vs RAM whenever possible
* Avoiding network costs by preferably talking to data sources in the same AZ
* Avoiding memory copies and more generally keeping memory footprint predictably low during both serving and write side pipelines
* Not relying on Spark or other JVM based systems that can do a lot of data shuffling and hence need lots of RAM
* ...and lot more
