---
title: Delete Branch
order: 0
status: published
---

`delete_branch`
### Delete Branch
Deletes an existing branch and checks out the client to point to the `main` branch.

#### Parameters
<Expandable title="name" type="str">
The name of the existing branch that should be deleted. 
</Expandable>

#### Errors
<Expandable title="Branch does not exist">
Raises an error if a branch of the given `name` doesn't exist.
</Expandable>

<Expandable title="Invalid auth token">
Raises an error if the auth token isn't valid. Not applicable to the mock client.
</Expandable>

<Expandable title="Insufficient permissions">
Raises an error if the account corresponding to the auth token doesn't carry 
the permission to delete branches. Also raises an error if the token doesn't have
edit access to the entities in the branch being deleted. Not applicable to the 
mock client.
</Expandable>

<pre snippet="api-reference/client/branch#delete_branch" status="success"
    message="Delete an existing branch">
</pre>
