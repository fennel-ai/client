---
title: 'Deployment Model'
order: 2
status: 'published'
---

# Deployment Model

The primary deployment model is to run Fennel in a fully managed mode inside 
your VPC as a single tenant system -- as a result, the data never leaves your
cloud and is fully covered by your existing infosec policies. Over time, a hosted
multi-tenancy system will also be made available.


**How does deployment work inside your VPC?**

You create a sub-organization in your cloud and give Fennel some privileges to it.
Fennel can then bring up machines/services and scale things up and down as
needed to handle the traffic. Fennel cluster inside your VPC talks to Fennel control
plane only to exchange telemetry information and control instructions (e.g. control plane
may choose to downscale the dataplane when traffic is lower, say at night). Either way,
your code and data never leave your cloud premise.

Since Fennel is limited to a sub-organization, it has no visibility / access into the
rest of your cloud.
