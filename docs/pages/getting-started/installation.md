---
title: 'Installation'
order: 2
status: 'published'
---

# Installation

Fennel has a client/server model. A Python client is used to define the 
features, which are then synced with the server. The server is responsible for 
computing, storing, and serving the actual features.

## Installing Fennel's Python Client

The client can simply be pip-installed like this:

```bash
pip install fennel-ai
```

The client itself ships with an in-memory mock server which has near feature
parity with the real server. All the code samples in the documentation can be 
run just via the mock server. See [quickstart](/getting-started/quickstart) 
for an example. 

In other words, once you have pip installed the client, you are all setup to 
explore Fennel. 


## Deploying Fennel Server

Fennel runs as a single tenant system inside your cloud so that your data and 
code never leave your cloud perimeter. As an example, here are the steps for 
deployment on AWS:

1. Create a new account in
   your [AWS Organization](https://docs.aws.amazon.com/organizations/latest/userguide/orgs_introduction.html). Fennel
   will run inside this account.
2. Fennel support will give you a cloudformation template - run that from the 
   region/account chosen in step 1. This template provisions a role and an IAM 
   permission boundary.
3. Share the region, account ID, and ARN(s) of the IAM role & the permission
   boundary created by the cloud formation template with Fennel support.
4. In addition, agree on a VPC CIDR that Fennel should use for the data plane, 
   whether you want the endpoints to be public or private, and the email domain(s)
   that should be allowed to access Fennel web console. 
5. Sit back as Fennel Support deploys the platform in this account and manages
   it from there. When done, Fennel will give you URLs of the Fennel console,
   the main data/query servers and the VPC ID of the VPC setup in the account.
5. If you want Fennel to ingest data from a private resource (e.g. RDS, Snowflake) 
   in any of your accounts, you may need to do VPC peering between the VPC
   set up by Fennel and your other AWS accounts. If you choose to use public 
   endpoints instead, Fennel support can provide IPs to be added to 
   appropriate allowlists on your side (for Snowflake access, for instance).
6. To enable laptop access of Fennel for your team, Fennel can either provide
   PrivateLink endpoint or you can use VPC peering and/or VPN setup yourself.
7. Once you have access to console URL and network connectivity, visit the 
   Fennel console to generate access tokens (you can also create new RBAC 
   roles if needed and/or assign users to roles).
9. Instantiate Fennel client by providing server URL and access tokens. Use this
   client to talk to the server.
10. You are all set now!

