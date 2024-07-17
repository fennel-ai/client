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
explore Fennel locally without needing a Fennel server.


## Deploying Fennel Server

Fennel runs as a single tenant system inside your cloud so that your data and 
code never leave your cloud perimeter. See [deployment model](/security-compliance/deployment-model)
for details. 

With this deployment model, here are the steps for deploying Fennel servers in 
your cloud:

1. Create a new account in
   your [AWS Organization](https://docs.aws.amazon.com/organizations/latest/userguide/orgs_introduction.html) 
   and note down the account ID. Fennel will run inside this account. 
2. Create a VPC in this account with internet and nat gateways configured so 
   that nodes have internet access. The VPC subnets also need to be 
   appropriately tagged for load-balancer creation. You can do so by following 
   [this guide](https://docs.aws.amazon.com/eks/latest/userguide/creating-a-vpc.html)
   or Fennel support can also provide standard cloud-formation templates to use.
3. Once VPC is setup, share the account ID from step 1 with the Fennel support. 
   Also inform Fennel support a) whether you want the endpoints to be public 
   or private and b) the email domain(s) that should be allowed to access Fennel 
   web console.
4. In response, you’d be given a cloud formation template and a cluster ID. Run 
   the cloud formation template from the account chosen in step 1 with the 
   following inputs: a) VPC: VPC ID, publicSubnets, privateSubnets, 
   cidr b) cluster ID
5. This template provisions the outpost EC2 instance(s) inside the VPC, a 
   security group, couple of IAM roles, a role permission boundary and some 
   other machinery.
6. Sit back as Fennel Support deploys the platform in this account and manages 
   it from there. When done, Fennel will give you URLs of the Fennel console 
   and the main API servers.
7. If you want Fennel to ingest data from a private resource (e.g. RDS, 
   Snowflake) in any of your accounts, you may either need to peer the 
   VPC set up in step 2 with your other AWS accounts or add the IP of the NAT 
   gateway to appropriate allow lists (e.g. Snowflake)
8. To enable laptop access to Fennel for your team, Fennel can either provide 
   PrivateLink endpoint or you can use VPC peering and/or VPN setup yourself.
9. Once you have access to Fennel console, use it to generate access tokens (you 
   can also create new RBAC roles if needed and/or assign users to roles).
10. Instantiate Fennel client by providing server URL and access tokens. Use 
    this client to talk to the server.

And you are all set now!

