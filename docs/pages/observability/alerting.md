---
title: Alerting
order: 4
status: 'published'
---

# Alerting

Inspired by Unix Philosophy of doing few things well and composing with other 
more specialized tools, Fennel's approach to alerting is to provide the raw
metrics behind a standardized [prometheus endpoint](/observability/prometheu) so 
that you can use your native alerting tools to build any desired alerts yourself.

And since Fennel is completely managed, you don't need to be on PagerDuty for 
incidents affecting the Fennel system itself - Fennel engineers get paged for 
those via internal systems. That said, Fennel still provides you a mechanism 
to page Fennel engineers for any unaddressed production critical issues.