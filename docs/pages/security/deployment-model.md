---
title: 'Deployment Model'
order: 3
status: 'published'
---

# Deployment Model


Fennel deployements follow data plane/control plane architecture as follows:

![Diagram](/assets/deployment_model.jpg)

## Data Plane
- Runs within your AWS organization - either as a dedicated account (preferred)
  or a shared account.
- Provisioning is bootstrapped through a cloud-formation template provided by
  Fennel support. After the initial bootstrap, the control plane finishes the 
  provisioning and manages the ongoing operations of the data plane.
- All your code, data, and secrets as well as all the compute live entirely in 
  the data plane.
- The web console (which also runs inside the data plane so that even your code
  doesn't have to leave the data plane), integrates with SSO and manages roles, 
  accounts, and tokens to provide fine grained column-level access control.
- Data plane integrates with your data sources to continuously ingest data
  for transformation. This may require VPC peering or IP allowlisting or other
  such mechanisms to give the data plane access to data resources.
- No network ports are left open and the communication with the control plane
  is done over a reverse tunnel initiated by the data plane.
- All data is encrypted, both in transit and at rest. 
- All inter-server communication happens over TLS. All communication between 
 your application and Fennel servers is authenticated via RBAC tokens and 
 also happens over TLS.


## Control plane
- Runs within the Fennel Cloud AWS account.
- Responsible for ongoing service upgrades via Fennel's CI/CD systems.
- Fennel services communicate with the data plane via a reverse SSH tunnel 
  established by the Outpost.
- All communications with the data plane use short-lived security certificates and
  a varying combination of MFA, SSO, just-in-time access approvals, and trusted 
  devices.