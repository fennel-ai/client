---
title: 'Core Engine'
order: 0
status: 'published'
---

# Core Engine

Here is a simplified diagram of what is happening inside each engine:

![Diagram](/assets/core_engine.jpg)

## Job

All your Fennel code is eventually compiled down into a set of jobs. A job is a 
process that reads from 0 or more internal Kafka topics and writes to 0 or more
internal Kafka topics. 

Jobs are partitioned, in fact co-partitioned with the input/output Kafka topics. 
As a result, a given partition of job only reads from some partitions of the 
input topics and write to some partitions of the output topic (the rules of 
which partition to read/write from are complex and depend on the type of job). A
job partition is the smallest schedulable unit of work in Fennel.

Some (but not all) jobs are stateful and are provided their own state store - 
Fennel uses a pretty generic & simple interface called Hangar that represents a
batch friendly K/V store. Under the hood, Hangar is usually implemented as a
RocksDB instance.


## Job Supervisor

Every engine runs one async event loop called _Job Supervisor_ that is polling 
the job registry every few seconds to notice any changes. If it finds any jobs 
that have been recently added or removed, it spins up (or down) an event loop 
for each partition of the job. If more than one engine is running in the cluster 
(which is the common case), only some partitions of some jobs will belong to a 
given engine - that is determined by a process akin to consistent hashing.

Once supervisor identifies a new job that must be added and concludes that it 
should own one or more partition of the job, it spins up Runner for those 
partitions. Conversely, supervisor can spin down existing runners if the job
should not be run any more.


## Job Runner
Like Supervisor, Runner is also an async event loop. However, a Runner is tied
to a particular partition of a particular job. Runner maintains all in-memory
state relevant for the job iterations, which usually is nothing more than 
connections to input/output Kafka and handle to a RocksDB instance.

Runner is responsible for almost all the IO. In fact, it starts each iteration
of the job by first choosing one of the input Kafka partitions to read from,
reading a batch of messages from it and then transfer the control to the actual
logic of the job for one iteration. Assuming job run is successful, job returns
the list of output messages as well as any new state changes to be written back
to RocksDB.

Runner then initiates a Kafka transaction and writes all output messages to the 
appropriate partition of output topic(s). Runner also maintains a single-partition
topic called _replaylog_. Runner writes a special marker message in the replaylog
that somehow encodes & identifies the state store mutations (but doesn't 
actually write state store changes in Kafka). Once the transaction is committed,
Runner applies the same state mutations to its own copy of RocksDB along with 
some metadata (e.g. the offset of input topics to which data has been processed).

Fennel's usage of Kafka transactions also provides the exactly-once-processing 
guarantee.


## Job State

All job state is maintained in a local RocksDB instance which is periodically
backed to S3. In addition to the actual state, this K/V store also maintains some
special metadata (e.g. offset up to which data has been read and reflected in this
RocksDB).

During recovery, an older snapshot of RocksDB (possibly empty) is restored from 
S3. Using it and the contents of the replaylog topic, the Runner may decide to 
replay some of the input messages again. Note that replay is just for populating 
the state but the output messages aren't sent to output Kafka topics (because 
that has already happened before). Since state snapshots are taken periodically,
often very little data needs to be replayed, leading to fast recoveries.
