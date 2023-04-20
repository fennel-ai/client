---
title: 'Deployment Model'
order: 1
status: 'published'
---

# Deployment Model

The primary deployment model is to run Fennel in a fully managed mode inside your VPC as a single tenant system \[1] -- as a result, the data never leaves your cloud and is fully covered by your existing infosec policies.&#x20;

Over time, a hosted multi-tenancy system will also be launched.&#x20;



**How does deployment work inside your VPC?**

You create a sub-organization in your cloud and give Fennel admin privileges to it. Fennel's automation can bring up machines/services and scale things up and down as needed to handle the traffic. Fennel cluster inside your VPC talks to Fennel control plane to exchange telemetry information and control instructions (e.g. control plane may choose to downscale the dataplane when traffic is lower, say at night). Either way, your data doesn't leave your cloud premise.

Since Fennel is limited to a sub-organization, it has no visibility / access into the rest of your cloud.&#x20;



\[1] If it makes your life easier, we are also happy to spin up a separate AWS organization ourself and run your system inside it. That way, you'd effectively get a single tenant system outside of your VPC.
