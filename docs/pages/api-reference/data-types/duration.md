---
title: Duration
order: 2
status: 'published'
---

### Duration
<Divider>
<LeftSection>

Fennel lets you express durations in an easy to read natural language as described below:

| Symbol | Unit   |
| ------ | ------ |
| y      | Year   |
| w      | Week   |
| d      | Day    |
| h      | Hour   |
| m      | Minute |
| s      | Second |


There is no shortcut for month because there is a very high degree of 
variance in month's duration- some months are 28 days, some are 30 days and 
some are 31 days. A common convention in ML is to use `4 weeks` 
to describe a month.

:::info
A year is hardcoded to be exactly 365 days and doesn't take into account
variance like leap years.
:::

</LeftSection>
<RightSection>
```text
"7h" -> 7 hours
"12d" -> 12 days
"2y" -> 2 years
"3h 20m 4s" -> 3 hours 20 minutes and 4 seconds
"2y 4w" -> 2 years and 4 weeks
```
</RightSection>
</Divider>