import unittest

import pandas as pd

import feature
import workspace


class MyTest(unittest.TestCase):
    def test(self):
        pass

class MyTest(unittest.TestCase):
    def test(self):
        with workspace.Test() as ws:
            ws.add_stream('mystream')
            ws.add_aggregate('myaggregate')
            ws.add_feature_packs(...)
            ws.sync()

            ws.log(...)
            # TODO(aditya): how do we tell the system to wait until aggregates are processed
            ws.extract(...)