---
title: Ownership
order: 1
status: 'published'
---

# Ownership

Enforcing ownership of code is a well known approach in software engineering
to maintain the quality & health of code. The same is true for production ML
systems too but most organizations don't enforce ownership of pipelines or 
features.

Fennel requires that every dataset and feature has an explicit owner email and 
routes alerts/notifications about those constructs to the owners. As people
change teams or move around, this makes it more likely that context will be 
transferred.

Fennel also makes it easy to identify downstream dependencies - e.g. given a 
dataset, it's easy to see if any other datasets or features depend on it. 
Knowing that a construct has truly no dependencies makes it that much easier 
for teams to simply delete them on ownership transitions vs keeping them around.

Ownership in itself doesn't magically prevent any quality issues but hopefully should lead to subjectively
higher hygiene for code and data.