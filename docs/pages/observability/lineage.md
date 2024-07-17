---
title: Lineage
order: 9
status: 'published'
---

# Lineage

Fennel console contains an interactive visualization of the full lineage graph
spanning all Fennel entities - data sources, datasets, features, extractors etc. 
This lineage information is inferred automatically from the program structure 
without any manual annotation.

![Diagram](/assets/lineage.png)

This visualization can be used to discover datasets/features of interest. It can
also be used to understand the relationships between existing entities - which can
further help debug issues and/or build deeper intuition about the dataflow.

Fennel also exposes all the lineage information behind a [REST API](api-reference/rest-api/lineage) 
in case you want to combine this with broader data lineage graphs outside of Fennel and host
it in your own dashboards.