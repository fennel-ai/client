---
title: 'Key Ideas'
order: 1
status: 'published'
---

# Key Ideas

Here are some of the key ideas & principles behind Fennel's architecture that
allow it to meet its [design goals](/):


## Async Rust (using Tokio)

All Fennel services are written in Rust with tight control over CPU and memory
footprints. Further, since feature engineering is a very IO heavy workload on
both read and write sides, Fennel heavily depends on async Rust to release CPU
for other tasks while one task is waiting on some resource. For instance, each
`job` in Fennel's streaming system gets managed as an async task enabling many
jobs to share the same CPU core with minimal context switch overhead. This
enables Fennel to efficiently utilize all the CPU available to it.


## Python Native; Embedded Python

Fennel is one of the very few Python native streaming systems out there and that's
a natural consequence of its primary design goal of focusing on the ease of use. 
Use of Python (vs say SQL) also makes it easy to write unit tests, decompose 
code in function/class units, write native Python as UDFs etc.

But this requires lot of back and forth between Python land (which is bound by 
GIL) and the rest of the Rust system.  Fennel handles this by embedding a Python 
interpreter inside Rust binaries (via PyO3). This enables Fennel to cross the 
Python/Rust boundary very cheaply, while still being async.  

[PEP 684](https://discuss.python.org/t/pep-684-a-per-interpreter-gil/19583)
further allows Fennel to eliminate GIL bottleneck by embedding multiple
Python sub-interpreters in Rust threads.


## Read Write Separation; Provides Read Side Indices

It is rather common for people to do use streaming engine to output data into a
data sink and then build an index on top of that sink for low latency production 
serving. Similarly, on the feature engineering side, the computation is somewhat
divided across read & write sides.

Under this lens, there is a continuum between write side & read side computation
with various applications choosing their own combination of the two. Recognizing 
this common pattern, unlike most other compute systems out there, Fennel provides 
read side indices and computation out of the box.

This is one of the most critical architecture decision that differentiates
Fennel from many other similar systems. You can read about this in more detail
[here](/architecture/read-write-separation).


## Exposes Only Dataframe API

Some streaming systems like Flink or Bytewax support streams of arbitrary things
including, for instance, a stream of numbers or words. Sometimes, as in the case
of Flink, a higher level dataframe API is exposed on top of the lower level stream
API.

Fennel takes a different route - it only exposes a dataframe API i.e. all 
streams are "rows" of data with a schema, mandatory time field and zero or more 
key fields. This makes Fennel less useful for some lower level streaming tasks
but easier/friendlier for data engineering/data science tasks that typically
work on 2d dataframes.


## Kappa Architecture

Fennel uses a Kappa like architecture to operate on both streaming and batch data
via the same programming model. This enables Fennel to maintain a single code 
path to power all data operations and have the same pipeline declarations work 
seamlessly in both realtime and batch sources. This side-steps a lot of issues 
intrinsic to the Lambda architecture, which is the most mainstream alternative 
to Kappa. 

Once again, this makes Fennel easier to use, especially when dealing
with mixture of batch and streaming data. However it does come at a cost - more 
complexity has to be pushed deep in the internals of Fennel.


## CDC Awareness

Fennel transparently handles change data capture records aka CDC without exposing
those details to the end user (i.e. the end user still specifies the computation
declaratively and Fennel translates it into computation over stream of inserts & 
deletes). 


## Long Watermarks; Eager Emissions; Eventual Corrections

Most streaming systems buffer input data for a while (say until the window 
closes) and only then emit the final results. This has two downsides:
1. The results are delayed
2. Often, this temporary state needs to be kept in RAM

Both of these are fine with short windows. However, over time, it has been
noticed that in several real world applications, the data can continue getting
updated for very long periods (e.g. 30 days for financial transaction data). 
Clearly, it's a bad idea to delay results by 30 days OR store 30 days worth of 
state in RAM.

Fennel's streaming engine is built for these very long watermark periods. In 
particular, Fennel operators emit results eagerly whenever they encounter new 
data - even before the window has closed. But to handle changes due to new 
arriving data, Fennel remembers some state on Disk (not RAM) and then emits
corrections later. These corrections themselves look like CDC data stream. This
way, Fennel chooses to keep small delay times low and small memory footprint by
trading off on the volume of updates.


## Hybrid Materialized Views

To reduce read latencies, data on the write path is pre-materialized and stored
in datasets. The main downside of materializing views is that it may lead to
wasted computation and storage for data that is never read. Fennel's read write
separation minimizes this downside by giving control to the end user for what
computation to pre-materialize and what computation to on the read path.


## Minimal Sync Communication for Horizontal Scaling

Every single subsystem within Fennel is designed with horizontal scalability in mind.
While ability to scale out is usually desirable, if not done well, lots of independent
nodes can lead to inter-node overheads leading to capacity of the system not growing
linearly with hardware capacity. It also creates failure modes like cascades of failures.

Fennel minimizes these by reducing cross-node sync communication - it does so by keeping
some local state with each node (which needs no communication), keeping global metadata
in centrally accessible Postgres, and making all communication async - within node
communication via async Rust channels and cross-node communication via Kafka (vs sync RPCs)


## Transmit Objects & Protobufs; Not Raw Code

During `commit`, Fennel client doesn't send raw source files to the server. Instead,
Python objects are converted to protobufs which are then sent to the server. 
This is a subtle but important distinction - the code submitted to Fennel may 
come from any place, maybe scattered throughout a repo, or maybe dynamically 
generated at runtime. This choice also keeps the door open for us to extend 
Fennel in other languages (e.g. Typescript, Java), which we intent to do at some
point in time.