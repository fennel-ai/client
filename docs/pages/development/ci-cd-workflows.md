---
title: CI/CD Workflows
order: 2
status: 'published'
---

# CI/CD Workflows

Deployment to Fennel can be done via Github actions (or any other post-merge) 
script. Credentials to Fennel servers are typically injected as environment
variables in this script such that when `sync` call is made, the client ends up 
connecting with the chosen Fennel servers. 

## Example: Github Actions

Say we have a single dataset as follows in a file called `datasets.py`:

<pre snippet="testing-and-ci-cd/ci_cd/datasets#gh_action_dataset"></pre>

And a single featureset as follows in a file called `featuresets.py`:
<pre snippet="testing-and-ci-cd/ci_cd/featuresets#gh_action_featureset"></pre>

A simple Python script could be written that imports both the dataset & featureset
modules, reads Fennel URL & Token as environment variables and makes a sync
call.
<pre snippet="testing-and-ci-cd/ci_cd/sync#gh_action_sync"></pre>

Given such a setup, here is a sample Github actions script to deploy changes to
Fennel servers:
```bash
name: sync

on: # deploy only on pushes to the main branch
  push:
    branches:
      - main

jobs:
  fennel-sync:
    runs-on: ubuntu-latest
    name: Sync datasets and features to Fennel cluster.
    steps:
      - name: Checkout Repository
        uses: actions/checkout@v2

      - name: Setup Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.11.0

      # Depending on your setup of Fennel, you may need to setup a VPN 
      # connection here to talk to Fennel from Github action workers.
      # You may also need to install more pip packages here depending 
      # on what else is needed to import your Python modules.

      # Install fennel and sync against the cluster
      - name: Sync Datasets and Features
        env:
          FENNEL_URL: ${{ secrets.FENNEL_URL }}
          FENNEL_TOKEN: ${{ secrets.FENNEL_TOKEN }}
        run: |
          python3 -m pip install --upgrade pip
          pip install "fennel-ai>=0.19.0"
          python3 -m path.to.fennel.code.sync
```


## Deployment Variations

1. **Separate environments for dev, staging, prod etc.** - you can create multiple
   Fennel deployments, one each for dev, staging, prod etc. Each of these will 
   have their own URL & tokens. You can modify the script to just inject the
   URL/token of an appropriate deployment at the right time.
3. **Canary based deployment** - you can modify the above script to first deploy
   to a canary deployment of Fennel (by injecting appropriate URL/token), run
   whatever sanity checks you want to run, and then proceed with deployment to
   prod after that.
4. **Post-code review deployments** - you can change the trigger of the script
   above such that it runs only at a stage where all code reviews have been 
   resolved (e.g. changing the step or branch name)
5. **Disabling prod deployment outside of CI/CD** - Fennel supports fined-grained
   RBAC using which you can control permissions of tokens. For instance, you can
   specify that only the tokens of a particular role (say super admin) can deploy
   to Prod and all other tokens can only read feature values. With this, as long
   as that token is only available in your CI/CD environment, your team members
   can still see/query prod Fennel but not modify it without having gone through 
   CI/CD first.


## Stateful Rollbacks

Unlike Git, Fennel doesn't have a direct operation for reverting deployments. 
This is the case because dataset/featureset changes create some state - for instance,
some data might have been ingested in a dataset already - should it be deleted 
during roll back or not. In that sense, it is somewhat similar to roll backs of 
database schema migrations.

Consequently, the best way to roll back is to not blindly revert the commit but 
rather explicitly mark things as deprecated or deleted before syncing again.

To prevent accidental deletions of entities, Fennel requires you to first do a
sync with datasets/featuresets marked as deleted and then in subsequent syncs,
you are free to omit the entities from the sync call itself. Here is a simple
example showing how to mark dataset (or featureset) as deleted.

<pre snippet="testing-and-ci-cd/ci_cd/datasets#dataset_deleted" highlight="5"></pre>