---
title: 'Privacy & Security'
order: 3
status: 'published'
---

# Privacy & Security

### General InfoSec

Fennel runs inside your VPC and thus is subject to your usual infosec policies.
The code or data never leave your cloud which eliminates many privacy/compliance
vulnurabilities. 

While Fennel's control plane runs outside of your VPC, it only has access to logs
and telemetry information.


### Data security

Fennel uses industry best-practices for data security:

* All data is encrypted in transit and at rest
* Secure strorage of user-provided secrets in secrete stores or encrypted disks
* All inter-server communication happens over TLS
* Authentication and TLS for client-server requests