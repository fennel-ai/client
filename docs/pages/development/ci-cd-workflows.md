---
title: CI/CD Workflows
order: 2
status: 'published'
---

# CI/CD Workflows

### Philosophy

Fennel's core CI/CD philosophy is that deploying changes to Fennel should be _identical_ to the CI/CD process of the rest of your codebase - this way, your existing release processes should all continue to work. This is in contrast to alternatives where Fennel introduces its own rigid CI/CD process which may or may not work with your existing workflow.

This is achieved because `sync` calls are regular Python code in your regular code repo (vs a separate Fennel repo) and their behavior can be modified by changing the credentials of the server to which the client is connecting with.&#x20;

Here is one recommended workflow:

1. **Local Development** doesn't touch Fennel servers at all. Developers write new datasets/featuresets and verify that they work by writing unit tests (which provide a nearly complete mock of the real server except connectors to external datasets).&#x20;
2. **Deployment to Fennel** happens in a Github actions (or any other post-merge) script. Credentials to prod Fennel servers are injected as environment variables in this script such that when `sync` call is made, the client ends up connecting with the prod servers. Doing this post-merge ensures that only reviewed code modifies the state

This is just one workflow and you can create any workflow by appropriately injecting credentials to Fennel's prod clusters in the right place.&#x20;

For instance, another option is to have 3 Fennel clusters - dev, staging, prod. Individual developers have access to `dev` cluster which they can change during local development as they want. Credentials to `staging` tier are injected when the rest of the code reaches the staging layer. And the credential of prod cluster are injected when the code is ready to be deployed to prod.&#x20;

### Stateful Rollbacks

Sometimes changes need to be rolled back - this is a bit tricky because dataset/featureset changes create some state - sometimes explicitly (e.g. you might have logged data in a dataset already - should it be deleted at roll back) and sometimes implicitly (e.g. you might have used a particular `id` for a feature - does rollback release that id for others to reuse?). In that sense, it is somewhat similar to roll backs of database schema migrations.&#x20;

Consequently, the best way to roll back is to not blindly revert the commit but rather explicitly mark things as deprecated or deleted before syncing again.&#x20;
