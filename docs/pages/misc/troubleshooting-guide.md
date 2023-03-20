---
title: Troubleshooting Guide
order: 1
status: 'published'
---

# Troubleshooting Guide

<details>

<summary>I am having issues in connecting my MySQL DB to Fennel</summary>

Some users have reported that they could not connect to Amazon RDS MySQL or MariaDB. This can be diagnosed with the error message: `Cannot create a PoolableConnectionFactory`. To solve this issue please set **`jdbc_params` ** to **** `enabledTLSProtocols=TLSv1.2`&#x20;

</details>

