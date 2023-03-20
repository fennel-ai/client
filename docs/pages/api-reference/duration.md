---
title: Duration
order: 2
status: 'published'
---

# Duration

Describing some time duration is surprisingly common in feature engineering workflows. Fennel lets you express durations in an easy to read natural language as described below&#x20;

<table><thead><tr><th>Signifier</th><th>Unit</th><th data-hidden></th></tr></thead><tbody><tr><td>y</td><td>Year</td><td></td></tr><tr><td>w</td><td>Week</td><td></td></tr><tr><td>d</td><td>Day</td><td></td></tr><tr><td>h</td><td>Hour</td><td></td></tr><tr><td>m</td><td>Minute</td><td></td></tr><tr><td>s</td><td>Second</td><td></td></tr></tbody></table>

Examples:

1. "7h" -> 7 hours
2. "12d" -> 12 days
3. "2y" -> 2 years
4. "3h 20m 4s" -> 3 hours 20 minutes and 4 seconds
5. "2y 4w" -> 2 years and 4 weeks

:::info
A year is not a fixed amount of time but is hardcoded to be exactly 365 days
:::

Note that there is no shortcut for month because there is a very high degree of variance in month's duration- some months are 28 days, some are 30 days and some are 31 days. A common convention in feature engineering is to use `4 weeks` to describe a month.
