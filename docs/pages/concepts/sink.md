---
title: 'Sink'
order: 0
status: 'published'
---

# Sink

You can export data out of Fennel datasets by defining Sinks. Change data capture are published through 
a Kafka topic using debezium format.  

<pre snippet="concepts/introduction#sink"></pre>

In this example, first an object is created that knows how to connect with your
Kafka cluster. Then we specify the topic within this Kafka (i.e. `user_location`) 
that we intend to write data to. And finally, a decorator is added on top of the 
dataset which indicate that we should write the output of this dataset to Kafka topic.

And that's it - once this is written, `UserLocationFiltered` dataset will start 
publishing changes to your Kafka.

### CDC
Fennel publish changes to sinks on changes in the dataset. There's a few way you can read these changes:

* `debezium`:  Output change data capture (CDC) log in Debezium format.

We will add more CDC strategy in the future updates