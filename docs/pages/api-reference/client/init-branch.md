---
title: Init Branch
order: 0
status: published
---

`init_branch`
### Init Branch

Creates a new empty branch and checks out the client to point towards it.

#### Parameters
<Expandable title="name" type="str">
The name of the branch that should be created. The name can consist of any alpha
numeric character `[a-z, A-Z, 0-9]` as well as slashes `"/"`, hyphens `"-"`, 
underscores `"_"`, and periods `"."`

</Expandable>

#### Errors
<Expandable title="Invalid name">
Raises an error if the name of the branch contains invalid characters.
</Expandable>

<Expandable title="Branch already exists">
Raises an error if a branch of the same name already exists.
</Expandable>

<Expandable title="Invalid auth token">
Raises an error if the auth token isn't valid. Not applicable to the mock client.
</Expandable>

<Expandable title="Insufficient permissions">
Raises an error if the account corresponding to the auth token doesn't carry 
the permission to create a new branch. Not applicable to the mock client.
</Expandable>

<pre snippet="api-reference/client/branch#init_branch" status="success"
    message="Create a new empty branch 'mybranch'">
</pre>
