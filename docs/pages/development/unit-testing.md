---
title: Unit Testing
order: 0
status: 'published'
---

# Unit Tests

Fennel's Python client ships with an (inefficient) mock server inside it - this
makes it possible to do local development and run unit tests against the mock
server to verify correctness. This works even if you don't have any remote 
Fennel server - heck it works even if you don't have internet.

This mock server has near parity with the actual server with one notable 
exception - it doesn't support data connectors to external data systems 
(after all, it is completely local with zero remote dependencies!)

Let's first see how it will work and later we will see a fully functional unit test example.

```python
from fennel.testing import mock


class TestDataset(unittest.TestCase):
    @mock
    def test_dataset(self, client):
        # client talks to the mock server
        # ... do any setup
        # commit the dataset
        client.commit(datasets=[User])
        # ... some other stuff
        
        # Log data to the dataset directly (ONLY for testing)
        log(User, pd.Dataframe(...))
        #       OR
        # Log data to the dataset via a webhook
        client.log("fennel_webhook", 'User', pd.Dataframe(...))
        # ... some other stuff
        found = client.query(...)
        self.assertEqual(found, expected)    
```

Here we imported `mock` from `fennel.testing`. This is a decorator which 
can be used to decorate test functions - and the decorator supplies an extra 
argument called `client` to the test. Once the `client` object reaches the 
body of the test, you can do all operations that are typically done on a real 
client - you can commit datasets/featuresets, log data, extract features etc.

You can bring data to a dataset in the mock server, by using the log
function from our testing library or by explicitly logging data to a
[webhook](/api-reference/source_connectors/webhook).



## Testing Datasets

For testing Datasets, you can use the [log](/api-reference/client/log) method 
of the client to add some local data to a dataset or use the log method
from our testing library and then query this or other 
downstream datasets using the [lookup](/api-reference/client/lookup) method.
Here is an end to end example. Suppose our regular non-test code looks like this:

<pre snippet="testing-and-ci-cd/unit_tests#datasets"></pre>

And you want to test that data reaching `RatingActivity` dataset correctly 
propagates to `MovieRating` dataset via the pipeline. You could write the 
following unit test to do so:
<pre snippet="testing-and-ci-cd/unit_tests#datasets_testing"></pre>

## Testing Featuresets
Extractors are simple Python functions and, hence, can be unit tested directly.

<pre snippet="testing-and-ci-cd/unit_tests#featuresets_testing"></pre>


For extractors that depend on dataset lookups, the setup looks similar to that 
of testing datasets as shown above - create a mock client, `commit` some 
datasets/featuresets, log data to a dataset, and finally use client to extract 
features. Here is an example:

<pre snippet="testing-and-ci-cd/unit_tests#featuresets_testing_with_dataset"></pre>
