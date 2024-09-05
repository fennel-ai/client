---
title: Checkout
order: 0
status: published
---

`checkout`
### Checkout

Sets the client to point to the given branch.

#### Parameters
<Expandable title="name" type="str">
The name of the branch that the client should start pointing to. All subsequent
operations (e.g. `commit`, `query`) will be directed to this branch.
</Expandable>
<Expandable title="init" type="bool" defaultVal="False">
If true, creates a new empty branch if the `name` is not found within the branches synced with Fennel
</Expandable>


<pre snippet="api-reference/client/branch#checkout" status="success"
    message="Changing client to point to 'mybranch'">
</pre>


#### Errors
`checkout` does not raise any error.

:::info
If not specified via explicit `checkout`, by default, clients point to the 'main' branch.
:::

:::info
Note that `checkout` doesn't validate that the `name` points to a real branch by default. Instead, it just changes the local state of the client. If the branch doesn't 
exist, subsequent branch operations will fail, not the `checkout` itself. However, when `init` is set to `True`, `checkout` will first create the branch if a real branch is not found and subsequently point to it.
:::