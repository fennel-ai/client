---
title: 'Installation'
order: 2
status: 'published'
---

# Installation

Fennel has a client/server model. A Python client is used to define the 
features, which are then synced with the server. The server is responsible for 
computing, storing, and serving the actual features.

Client can simply be pip-installed like this:

```bash
pip install fennel-ai
```

Currently, there is no self-serve option for provisioning a server - please
contact Fennel team (hello[at]fennel.ai) if you want to provision one. 
However, the client itself ships with a mock server which has near feature
parity with the real server. All the code samples in the documentation can be 
run just via the mock server. See [quickstart](/getting-started/quickstart) 
as an example.

In other words, once you have pip installed the client, you are all setup to 
explore Fennel.