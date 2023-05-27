---
title: 'Overview'
order: 0
status: 'published'
---

# Overview

Here are some of the key ideas & principles behind Fennel's architecture that 
allow it to meet its [design goals](/):

## Read Write Separation

This is arguably the most critical architecture decision that differentiates 
Fennel from many other similar systems. You can read about this in more detail 
[here](/architecture/read-write-separation).

## Kappa Architecture

Fennel uses a Kappa like architecture to operate on streaming and offline data.
This enables Fennel to maintain a single code path to power the data operations
and have the same pipeline declarations work seamlessly in both realtime and 
batch sources. This side-steps a lot of issues intrinsic to the Lambda 
architecture, which is the most mainstream alternative to Kappa.

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

## Async Rust (using Tokio)

All Fennel services are written in Rust with tight control over CPU and memory
footprints. Further, since feature engineering is a very IO heavy workload on 
both read and write sides, Fennel heavily depends on async Rust to release CPU
for other tasks while one task is waiting on some resource. For instance, each
`job` in Fennel's streaming system gets managed as an async task enabling many
jobs to share the same CPU core with minimal context switch overhead. This 
enables Fennel to efficiently utilize all the CPU available to it.

## Embedded Python

Fennel's philosophy is to let users write in real Python with their familiar 
libraries vs having to learn new DSLs. But this requires lot of back and forth
between Python land (which is bound by GIL) and the rest of the Rust system. 
Fennel handles this by embedding a Python interpreter inside Rust binaries 
(via PyO3). This enables Fennel to cross the Python/Rust boundary very 
cheaply, while still being async. [PEP 684](https://discuss.python.org/t/pep-684-a-per-interpreter-gil/19583) 
further allows Fennel to eliminate GIL bottleneck by embedding multiple 
Python sub-interpreters in Rust threads.
