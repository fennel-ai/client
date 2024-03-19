---
title: 'Storage Model'
order: 0
status: 'published'
---

# Storage Model

With the basics of [Fennel Architecture](/architecture/overview) and [Fennel's 
Core Engine](/architecture/core-engine) clarified, we can now talk about the
storage model of Fennel:

1. All Fennel datasets correspond to an internal partitioned Kafka topic. The 
   retention of this topic can be configured by user (though is usually long). 
2. Fennel creates several other internal Kafka topics - though these are usually
   for short durations.
3. Data for Kafka topics is tiered to S3. This way, majority of Kafka data lives
   in S3 at any point and only a small subset of it lives on local SSDs of Kafka
   brokers.
4. Pipelines get mapped to partitioned jobs which may have job specific local 
   state. This state lives in a RocksDB instance running on local SSD of the 
   engines with snapshots living in S3.
5. In addition to the RocksDB and S3, metadata about this state also lives in 
   a special internal Kafka topic (called `replaylog`), which again, is tiered
   to S3.
6. Indices are also mapped to partitioned jobs which also have their own local
   state (on RocksDB, snapshotted on S3 and metadata in Kafka) - the only 
   difference is that their local state is exposed for querying behind gRPC 
   endpoints.
7. All metadata (e.g. graph of all datasets, pipelines, indices, features etc.)
   as well as job registry lives in a centrally shared Postgres.