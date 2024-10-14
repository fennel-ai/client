---
title: Secret
order: 0
status: published
---

### Secret
Secret can be used to pass sensitive information like username/password to Fennel using Secrets Manager secret reference.

In order to use Secret one of the below should be followed:
1. Fennel Data access role (for all sources) and Glue Role (for any batch sources like S3, Redshift, MYSQL, etc.) should be given access to the secret. 
2. Or a new role can be created with access to secrets needed and Fennel Data access role and Glue role can be added as trusted entities for that new role. so that the new role can be assumed to access the secrets.


#### Parameters

<Expandable title="arn" type="str">
The ARN of the secret.
</Expandable>

<Expandable title="role_arn" type="Optional[str]">
The Optional ARN of the role to be assumed to access the secret.
This should be provided if a new role is created for Fennel Data access role and Glue role to assume.
</Expandable>

<pre snippet="api-reference/sources/kafka#secret"
    status="success" message="Using secrets with kafka"
></pre>