---
title: 'Deployment Model'
order: 2
status: 'published'
---

# Deployment Model

The primary deployment model is to run Fennel in a fully-managed mode inside
your cloud account as a single-tenant system. As a result, the data never leaves your
cloud.

**Deployment on AWS**

1. Create a new account in
   your [AWS Organization](https://docs.aws.amazon.com/organizations/latest/userguide/orgs_introduction.html).
2. Create a role in the new account and assign it admin privilege by attaching
   the
   AWS-managed [AdministratorAccess policy](https://docs.aws.amazon.com/aws-managed-policy/latest/reference/AdministratorAccess.html)
   to it
3. Allow Fennel to assume this role by adding `arn:aws:iam::030813887342:root`
   as a trusted entity for the role created in the previous step
4. Provide the AWS account id and role name to Fennel. Optionally, also provide a `/16` VPC CIDR
   block that does not overlap with any VPC on your end that we might want to peer with. Fennel will create a VPC in
   your account with this CIDR block.
5. Fennel deploys its Feature Engineering Platform in this account and manages
   it from there. To deploy and perform ongoing maintenance and upgrades,
   Fennel peers the deployed VPC with a VPC in our account (a.k.a. the control plane)
6. Connect the Fennnel service to your production/dev account via AWS PrivateLink
   or VPC peering. The latter may be required for Fennel to ingest data from
   some data sources in your production account.