---
title: 'Overview'
order: 3
status: 'published'
---

# Security Overview

At Fennel, safeguarding customer data is at the heart of our mission. It is 
imperative to cultivate a security-centric culture in order to gain the 
confidence of organizations addressing significant challenges. As a result, we 
infuse security best practices throughout all facets of our operations and 
product development, persistently striving to enhance our security measures.

Here is a non-exhaustive list of security best practices followed by our team:

## Culture of Security
### Security Program
Fennel is committed to a security program ensuring the confidentiality, 
integrity, and availability of customer data. Our information security policies, 
revised at least annually, are designed to implement best practices in areas 
such as access control, risk management, change management, incident response, 
and other vital aspects. These policies are based on established industry 
frameworks, and our controls have been rigorously certified through a SOC-2 
Type 1 assessment. Feel free to reach out to hello@fennel.ai to request a copy 
of our certified report.

### Our People
The foundation of Fennel's security program rests on its personnel. We uphold 
rigorous criteria for recruitment, conducting thorough background checks for 
all prospective employees. Regular performance assessments are conducted to 
verify alignment with our objectives and practices. Employees not adhering to 
policies may be subjected to disciplinary actions, including but not limited to termination.

### Awareness Training
Fennel upholds a Security Awareness initiative integrated into our onboarding 
process and annual reviews. Participation in this program is mandatory for all 
employees to guarantee their awareness and comprehension of the policies and 
procedures they are obligated to follow.

## Protecting Customer Data
### Network Security
Customer data or code never leaves the single-tenant data plane deployed inside the 
customer cloud. All data is encrypted in transit and at rest. Data plane has no
open ports and all the communication between the data plane and the control plane
happens over a secure tunnel initiated by the data plane.

Fennel hosts control plane infrastructure in AWS and leverages their physical 
and environment security controls.
![Diagram](/assets/deployment_model.jpg)

### Access Control
Fennel's control plane doesn't have the IAM permissions to access any resources
in the data plane hosted in the customer's cloud. Access to Fennel's production 
environment is role based using Google SSO, is authenticated via short-term 
certifiates, and follows the principle of least required priviliges at all times.

Elevated levels of access require just-in-time access request approvals such that
only the company founders can approve the access requests. Access controls are 
reviewed at least quarterly.

### Access Audits
All accesses to customer environments are logged and are periodically audited.
Realtime session activity for all sessions with elevated access levels are 
recorded.

## Data Security
### Encryption
Fennel uses AWS services to keep customer data encrypted during transit and 
at-rest. Data at rest is encrypted using AES 256-bit encryption and data in transit
is protected using TLSv1.2 or higher with requirement of modern cipher suites for
all connections.

### Data Retention and Disposal
All customer data is backed up nearly continuously in an automated manner.

Our organization adheres to clear-cut policies governing data retention and 
deletion. We keep customer data for the duration necessary to meet contractual 
commitments or comply with regulatory standards. Corporate data is retained 
based on defined periods established by pertinent business stakeholders in 
alignment with business objectives.


## Monitoring and Risk Management
### System Monitoring
Fennel invests heavily in extensive monitoring, observability, and alerting 
for our production environments. We make use of several tools, including but not 
liited to AWS cloudwatch, Prometheus, Grafana, and PagerDuty.

Alerts are configured for important production systems and functionality. 
Critical alerts are actioned immediately, with alerts & pages going out to our 
24x7x365 on-call rotation.

### Subprocessor Management
Fennel employs a risk-based framework to evaluate third-party sub-processors 
and vendors. We scrutinize their privacy, security, and confidentiality 
procedures to ensure that they do not compromise our commitment to safeguard 
customer data and provide a consistently available service.

Vendor assessments occur on an annual basis, considering factors such as the 
sensitivity of stored data, the significance of our reliance on the service, 
and the reputation of the service provider. You can find the current list of 
our sub-processors [here](https://fennel.ai/legal/subprocessors).

### Change Management
Fennel utilizes an industry standard agile methodology for software development, 
and performs extensive code reviews and testing before each release.

We adhere to industry best practices, which include mandatory PR approvals, 
automated code review, a suite of integration and unit tests, and periodic release
cuts following manual QA & testing.

Developers are trained to adhere to secure coding guidelines, and are aware of 
the OWASP Top 10 issues. Security-sensitive code is always reviewed by domain 
experts.

## Incident Management
### Security Incidents
Fennel follows a documented Incident Response Plan which is aligned with the 
SANS Incident Handlers Handbook.

Our plan includes steps for initiating the response plan, escalation to relevant 
leadership stakeholders, triage, investigation, analysis, mitigation, 
restoration, and post-mortem. Our engineering team works to monitor potential 
security issues pro-actively, and takes all reports of security-related issues 
very seriously.

Our 24x7x365 on-call rotation is available for customers to report security 
incidents. Reports should be made to support@fennel.ai, or through other 
contracted support channels.

Fennel will notify impacted customers of any security issues in a timely fashion.

### Disaster Recovery
Fennel has developed a Business Continuity and Disaster Recovery Plan. We 
perform an annual review and trial run of the plan, and work to ensure that we 
are able to resume business operations from backup locations, establish 
communication among core team members, and perform backup restorations 
successfully.


## Conclusion

This page enumerated a non-exhaustive list of our security practices.

Our mission at Fennel is to support the world's most innovative organizations
in building & deploying state of the art machine learning system. And we fully
realize the responsibility it places on us to maintain a state of the art
security operation and earn the trust of our customers.

Please don't hesitate in reaching out to us at hello@fennel.ai if you had any
questions or suggestions about any of this.
