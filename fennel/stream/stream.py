import datetime
import inspect

import cloudpickle

from fennel.gen.services_pb2_grpc import FennelFeatureStoreStub
from fennel.gen.stream_pb2 import CreateConnectorRequest, CreateStreamRequest
from fennel.lib import schema
from fennel.utils import Singleton
from fennel.errors import SchemaException
from typing import List


class Stream(Singleton):
    name = None
    version = 1
    retention = datetime.timedelta(days=30)
    # by default, get all the data from 1st of Jan 2020
    start = datetime.datetime(2020, 1, 1)

    def schema(self) -> schema.Schema:
        raise NotImplementedError()

    def validate(self) -> List[Exception]:
        # Validate the schema
        exceptions = self.schema().validate()
        # Validate the populators(source + connector) functions
        for name, func in inspect.getmembers(self.__class__, predicate=inspect.isfunction):
            if hasattr(func, 'validate'):
                exceptions.extend(func.validate())
        return exceptions

    def register(self, stub: FennelFeatureStoreStub):
        # Register the connectors/populators one by one.
        # Go through all functions of the class and find the ones that can be registered (sources)
        source_requests = []
        connector_requests = []
        for name, func in inspect.getmembers(self.__class__, predicate=inspect.isfunction):
            if hasattr(func, 'register'):
                source_requests.append(func.create_source_request())
                connector_requests.append(CreateConnectorRequest(
                    name=self.func.__name__,
                    source_name=func.source.name,
                    source_type=func.source.type(),
                    connector_function=cloudpickle.dumps(func.populator_func),
                    table_name=func.table,
                ))

        req = CreateStreamRequest(
            name=self.name,
            version=self.version,
            retention=int(self.retention.total_seconds()),
            # Fix start time
            # start=self.start,
            schema=self.schema().to_proto(),
            sources=source_requests,
            connectors=connector_requests,
        )
        resp = stub.RegisterStream(req)
        return resp
