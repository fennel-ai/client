---
title: 'Formalism'
order: 0
status: "draft"
---

# Formalism

Fennel's datasets & pipelines are built on top of a rigorous formalism described
below.

## Datasets as Sequence of Rowsets

Fennel datasets do not represent a single set of rows (henceforth called as 
`rowset`). Instead, they represent full evolution of data over time. More 
specifically, datasets should be visualized as sequence of rowsets - one rowset
for each microsecond (the lowest granularity of time in Fennel).