---
title: Clone Branch
order: 0
status: published
---

`clone_branch`
### Clone Branch
<Divider>
<LeftSection>
Clones an existing branch into a new branch and checks out the client to 
point towards it.

#### Parameters
<Expandable title="name" type="str">
The name of the new branch that should be created as a result of the clone. 
The name can consist of any ASCII characters.
</Expandable>

<Expandable title="from_name" type="str">
The name of the existing branch that should be cloned into the new branch.
</Expandable>

#### Errors
<Expandable title="Destination branch already exists">
Raises an error if a branch of the same `name` already exists.
</Expandable>

<Expandable title="Source branch does not exist">
Raises an error if there is no existing branch of the name `from_branch`.
</Expandable>

<Expandable title="Invalid auth token">
Raises an error if the auth token isn't valid. Not applicable to the mock client.
</Expandable>

<Expandable title="Insufficient permissions">
Raises an error if the account corresponding to the auth token doesn't carry 
permissions to create a new branch. Also raises an error if the token doesn't have
access to entities defined in the source branch. Auth/permissions checks are not 
applicable to the mock client.
</Expandable>
</LeftSection>
<RightSection>
<pre snippet="api-reference/client/branch#clone_branch" status="success"
    message="Clone 'base' branch into 'mybranch'">
</pre>
</RightSection>
</Divider>