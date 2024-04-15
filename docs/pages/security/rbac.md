---
title: 'RBAC'
order: 3
status: 'published'
---

# Role Based Access Control

Fennel supports creating custom roles with extremely fine-grained "column" level 
access control. 

## Fennel's Access Control Model
The access boundary of any role in Fennel can be defined as any combination of 
the following three kinds of controls:

1. Tag controls
2. Modes of access
3. Branch controls

### 1. Tag Controls
Entities like datasets/features can be given tags and roles can be created 
that guard access to say datasets with a particular tag.

These tags are automatically inherited across the lineage graph. For instance, if
a dataset had a tag 'PII', any other derived dataset depending on it will 
automatically inherit 'PII' tag. This inheritance works recursively
across any number of hops. 

Sometimes it's desirable to terminate the propagation of a tag, say because PII
information has been sufficiently deanonymized. This can be accomplished by 
tagging the entity with '~tag' (i.e. same tag with but ~ sign). Let's seen an
example:

<pre snippet="security/rbac#tag_propagation"></pre>

In the above example, dataset `TxnByCity` inherits "PII" from `User` and "stripe"
from `Transaction`. However, the "PII" tag is canceled because it has "~PII" on 
itself. So finally, it has "~PII" and "stripe" tags.

Further, feature `UserFeatures.city` inherits "PII" from `User` and `UserFeatures.total_in_hometown`
inherits "PII" from `UserFeatures.city` and "~PII" & "stripe" from `TxnByCity` 
resulting in final tag of only "stripe".

### 2. Modes of Access
Fennel distinguishes between four distinct modes of access - a) read definitions, 
b) write definitions, c) read data, d) write data. 

That is, role A may be able to only read definition of a dataset, another role 
B may be able to both read/write the definitions but can not see any data 
whereas another role may be able to see data but not edit definitions.

### 3. Branch Controls
Roles can also be tied to specific branches, if desired. For instance, it's possible
to build a role that can see data in branches b1, b2 but can only see definitions
in another branch.

## Putting It Together
Here is a screenshot of the role creation flow from Fennel console showing
all three of these controls:
![Diagram](/assets/rbac.png)

Here are some examples of the kind of real world situations that can be modeled
via Fennel roles:

1. Only CI/CD system can deploy to prod Fennel, not individual developers. However,
   developers should still be able to see definitions. 
2. There is special PII data which can only be used by data scientists working 
   on safety problems but not by others. However, non-PII data should be visible
   to everyone. 
3. A multi-national company has offices in many countries. Some customer data is
   deemed sensitive and should only be visible to employees within that country.
   Non-sensitive data is visible to everyone across the globe.

## Service Accounts & Tokens
Fennel also supports creating service accounts (as opposed to user accounts) for
API access as well as creating/invalidating tokens that have the same permissions
as the account they are attached to.

All API access to Fennel servers require a valid token for authentication carrying
enough permissions needed for authorizing the action.