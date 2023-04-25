---
title: 'Read/Write Separation'
order: 1
status: 'published'
---

# Read/Write Separation

Unlike most (if not all?) other feature platforms out there, Fennel distinguishes between read and write path computation. This is so because these are very different from perf characteristics -&#x20;

* Write path is throughput bound but does not have latency pressure whereas Read path is very sensitive to latency but often operates on tiny batches.&#x20;
* Write path creates data and so can end up taking lots of storage space, some of which may be wasted if the data isn't actually read in a request. That storage space may be traded off with CPU by moving that computation to the read path which may repeat the same computation for each request.

What computation to do on write path vs read path is a very application specific decision and so Fennel gives you control on that decision - pre-compute some quantity on the write path by keeping it in a pipeline. Or compute it on the fly by writing it as an extractor.&#x20;

Note that in a majority of cases, you'd just want to move the computation to write side as a pipeline. But there are several reasons/cases where keeping the computation on read path may make sense. Here are some examples:

* **Sparsity** -- say you have a feature that does dot product between user and content embeddings. There are 1M users and 1M content. It's very wasteful to compute dot product between every pair of user/content because it will take a lot of storage and most of it will not be read ever. So it's better to lookup embeddings from datasets and do their dot product in the extractor.
* **CPU cost** -- say you have a model based feature and further it's a heavy neural network model. You could put it on the write path to save latency. But that also means that you're running this neural network on every row of dataset, most of which may never be seen in prod. So depending on the CPU vs latency tradeoff, it may make sense to keep it on the read path.&#x20;
* **Request properties** -- for features that depend on request specific properties e.g. "is the transaction amount larger than user's average over the last 1 day", you may have no choice but to look up some partially pre-computed info and do some read side computation it.&#x20;
* **Temporal features** - say you have a feature that represents user's age. The values of this feature update automatically with passing of time and so it's physically impossible to pre-compute this on the write path. A more natural way is to look up user's date of birth from a dataset and subtract that from the current time.
