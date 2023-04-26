---
title: Out of Order Data
order: 3
status: 'draft'
---

# Out of Order Data

Handling out of order data is a hard problem for all streaming systems. Fennel
has two key ideas that make this much simpler compared to other alternatives:

1. Fennel data is always time aware (since datasets have a `timestamp` field). 
2. The origin of out of order arrival of data lies with the end data sources