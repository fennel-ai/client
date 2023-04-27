---
title: 'Core Technologies'
order: 2
status: 'published'
---
# Core Technologies

While Fennel has innovated at several layers of abstraction and software
stack, it would not have been possible if it wasn't standing on the 
shoulders of the giants. 

Here is a (non-exhaustive) list of core technologies leverage by Fennel:

* Client is written in Python and backend is primarily written in Rust with 
 heavy reliance on Tokio's async runtime
* Postgres as central metadata store (but doesn't store any customer data)
* Kafka for all in-flow data. All streaming jobs read from and write to Kafka
* RocksDB for all most at-rest data storage (with small parts offloaded to 
 Redis)
* Airbyte for data connectors
* Pulumi for provisioning of infrastructure as code
* Kubernetes for maintaining the lifecycle of all running services
* Protocol buffers as data exchange formats and GRPC for writing services
* Pandas is used as the dataframe interface between the server and the
  user written Python code

We, the team behind Fennel, are grateful to developer communities behind all these
for producing such high quality software.