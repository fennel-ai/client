import datetime
import inspect

import cloudpickle

from fennel.gen.services_pb2_grpc import FennelFeatureStoreStub
from fennel.gen.stream_pb2 import CreateConnectorRequest
from fennel.lib import schema
from fennel.utils import check_response, Singleton


class Stream(Singleton):
    name = None
    version = 1
    retention = datetime.timedelta(days=30)
    # by default, get all the data
    start = datetime.datetime(2020, 1, 1)

    @classmethod
    def schema(cls) -> schema.Schema:
        raise NotImplementedError()

    def register(self, stub: FennelFeatureStoreStub):
        # Register the connectors/populators one by one.
        # Go through all functions of the class and find the ones that can be registered (sources)
        for name, func in inspect.getmembers(self.__class__, predicate=inspect.isfunction):
            if hasattr(func, 'register'):
                func.validate_fn()
                func.register(stub)
                request = CreateConnectorRequest(
                    name=self.name,
                    version=self.version,
                    source_name=func.source.name,
                    source_type=func.source.type(),
                    connector_function=cloudpickle.dumps(func.populator_func),
                    table_name=func.table,
                )
                resp = stub.RegisterConnector(request)
                check_response(resp)

        # Store schema for the stream
        print("Registered stream:", self.name)
