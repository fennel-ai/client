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
4. Provide the AWS account id and role name to Fennel.
5. If Fennel will be required to ingest data from a private resource (e.g. RDS) in any of your accounts, a VPC
   peering connection will be required. In this case, provide the VPC id of the VPC in which the production resource is
   hosted, as well as a `/16` CIDR block that does not overlap with any VPC on your end. Fennel will create a VPC with
   this CIDR block in the new account.
6. Sit back as Fennel deploys its Feature Engineering Platform in this account and manages
   it from there. To deploy and perform ongoing maintenance and upgrades,
   Fennel peers the deployed VPC with a VPC in Fennel's account (a.k.a. the control plane).
7. Access the Fennnel service from your production/dev account via AWS PrivateLink
   or VPC peering if a peering connection was established in step 5.