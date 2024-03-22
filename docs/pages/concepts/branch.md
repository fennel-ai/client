---
title: 'Branch'
order: 0
status: 'published'
---

# Branch

A branch is the unit of logical separation for Fennel datasets &
featuresets within a cluster. A cluster can have many branches and each branch
can have many independent datasets & featuresets. Fennel's branch semantics are 
heavily inspired by Git.

<pre snippet="concepts/branch#basic" message="Managing multiple branches"></pre>

In the above example, we have two branches - `branch1` and `branch2`. They both
have the same dataset but different featuresets. In general, their entity 
graphs are completely independent of each other and can be queried for features
independently as well. Queries are always made against a particular branch 
(which by default is the `main` branch).

At any point of time, a given client instance is pointing to a particular branch.
`checkout` merely changes the branch pointed to by the client. It's okay to have
multiple client objects with different branches checked out.

## Main Branch
By default, Fennel creates a default branch called "main". All branch operations
talk to this branch if not explicitly specified. The only thing special
about this branch is that it can not be deleted - outside of that, it's just a 
regular branch. All branches are equally suitable to be used in any production or
experimental setting.

## Branch Cloning
In addition to creating new empty branches via `init_branch` method on the client,
Fennel also supports cloning an existing branch. The new cloned branch gets an
independent copy of all the datasets/featuresets in the original branch. After
the clone, the datasets/featuresets in the new branch can be modified arbitrarily
without influencing the original branch.

<pre snippet="concepts/branch#clone" 
    message="Cloning branch1 into a new branch branch2">
</pre>

### Copy-on-Write Clone
Fennel datasets/featuresets are logically isolated across branches and can
be modified independently. However, behind the scenes, Fennel uses copy-on-write 
like semantics and maintains the same physical representation (for both storage & 
compute) for identical entities across branches.

As a result, cloning an existing branch is extremely cheap. This makes cloning
ideal for those workflows when you want to modify just a couple of datasets/featuresets
and keep the rest of the graph unchanged.

Note that the copy on write semantics don't depend on the branch being cloned. 
For instance, if you created an empty branch via `init_branch` and committed the
same dataset/featureset definitions to it explicitly, Fennel is smart enough to 
still use the same physical assets to power the two branches.

## Branch Management

![Diagram](/assets/list_branch.png)

### Fennel Console
You can also use Fennel console to browse through all the existing branches. For
any given branch, you can see all its datasets/featuresets, inspect live dataflow 
etc. as well as see the full commit log.


### Access Control
Cloning a branch requires read/write access to all tags used in the entities of
the original branch. Additionally, it's possible to setup RBAC system such that 
branch creation, cloning, and deletion require elevated permissions.

### Branch Deletion
Branches can also be deleted via `delete_branch` method on the client. It's advisable
to periodically prune unused/inactive branches so as to potentially free up 
underlying storage/compute resources. Note that due to copy-on-write, all branches
pointing to a dataset/featureset need to be deleted to clean up the underlying
resources.

## Branch Workflows

Branches are flexible enough to be adapted to many workflows. Here are some of
the most common examples.

### Dev Branch per Developer
It's easy to create one (or more) branch per developer - that way, developers can
do development against their branch without getting in the way of each other. When
ready, the changes can be committed to some common standard branches.

### Dev Branch per Git Branch
Many teams develop in project specific git branches. It's possible to create one
Fennel branch per git branch for do all Fennel related changes in that project.

### Dev Branch for Integration Tests
Fennel's mock client provides native support for Python unit tests. However, if
you want to set integration tests involving real server as well, you could write
some test fixtures that spin up a branch with a random name in the test/dev cluster,
execute the test in that branch and tear down the branch after the test is finished.

### Branch per A/B Test
All Fennel branches are equally production ready and can serve live production
traffic. As a result, it's possible to create Fennel branches for each variant of
an A/B test and have different users query different branches.

