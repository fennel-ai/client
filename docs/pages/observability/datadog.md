---
title: Datadog
order: 2
status: 'published'
---

# Integrating Fennel With Datadog

Fennel metrics can be exported to Datadog simply by pointing it to scrape from
the Fennel's OpenMetrics compatible metric endpoint. Here is how to do that:



1. The path of the metrics endpoint is `/api/v1/metrics`. So typically full URL
   will be `https://<your>.<cluster>.fennel.ai/api/v1/metrics`. Note this down
   to be used in later steps.
2. Visit Fennel console, go to Settings -> Role and create a Fennel role that 
   doesn't have any permissions attached to it - we will use it to read metrics. 
   Note that reading metrics in Fennel requires valid token for authentication 
   but doesn't need any permissions for authorization, which is why we are 
   attaching no permissions to this role.
    ![Diagram](/assets/metrics_role.png)
3. Create a service account with the role created in the previous step.
    ![Diagram](/assets/metrics_account.png)
4. Issue a token for the service account created in the previous step. 
    ![Diagram](/assets/metrics_token.png)
5. Copy the token and keep it handy, as it’s required in future steps
    ![Diagram](/assets/datadog_token_copy.png)
6. Run a Datadog agent that has network access to Fennel's VPC. You can 
   run a Datadog agent explicitly to monitor Fennel or you can also use an already 
   running Datadog agent.
7. Configure the agent from the previous step to ingest metrics from Fennel's metric 
   endpoint by editing the OpenMetrics configuration file, which comes along 
   with Datadog Agent and are usually found in the agent configuration directory. 
   In that directory, find a file named `openmetrics.d/conf.yaml` and add the 
   following content (while substituting the URL and the token from steps 1 & 5):
   ```
   instances:
    # The endpoint to ingest metrics from
    - openmetrics_endpoint: <metric URL from step 1>
        # List of metrics - we will scrape all metrics starting with fennel
        metrics:
        - fennel*
        # Optional prefix to prepend to all metrics, can be anything (as below)
        # but recommended to use name of Fennel environment like "prod" or "dev"
        namespace: "openai"
        headers:
            Authorization: 'Bearer <auth-token from step 5>'
    ```
8. After modifying the configuration, restart the Datadog agent. Fennel metrics
   should start appearing on the Datadog metrics page and look something like
   this:
    ![Diagram](/assets/datadog_metrics.png)