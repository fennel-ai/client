import datetime
import inspect
from typing import List

import cloudpickle

from fennel.gen.services_pb2_grpc import FennelFeatureStoreStub
from fennel.gen.stream_pb2 import CreateConnectorRequest, CreateStreamRequest
from fennel.lib import Schema


class Stream:
    name = None
    version = 1
    retention = datetime.timedelta(days=30)
    # by default, get all the data from 1st of Jan 2020
    start = datetime.datetime(2020, 1, 1)
    schema: Schema = None

    @classmethod
    def validate(cls) -> List[Exception]:
        # Validate the schema & populators(source + connector) functions
        exceptions = []
        for name, func in cls.__dict__.items():
            if hasattr(func, "validate"):
                exceptions.extend(func.validate())
        # Ensure that timestamp is present in the schema
        exceptions.extend(cls.schema.check_timestamp_field_exists())
        return exceptions

    @classmethod
    def register(cls, stub: FennelFeatureStoreStub):
        # Register the connectors/populators one by one. Go through all
        # functions of the class and find the ones that can be registered (
        # sources)
        source_requests = []
        connector_requests = []
        for name, func in cls.__dict__.items():
            # Has a source attached to it
            if hasattr(func, "__func__") and hasattr(
                func.__func__, "create_source_request"
            ):
                func = func.__func__
                source_requests.append(func.create_source_request())
                connector_requests.append(
                    CreateConnectorRequest(
                        name=cls.name + ":" + func.func_name,
                        source_name=func.source.name,
                        source_type=func.source.type(),
                        connector_function=cloudpickle.dumps(func),
                        populator_src_code=inspect.getsource(func),
                        table_name=func.table,
                    )
                )
        req = CreateStreamRequest(
            name=cls.name,
            version=cls.version,
            retention=int(cls.retention.total_seconds()),
            # Fix start time
            # start=self.start,
            schema=cls.schema.to_proto(),
            sources=source_requests,
            stream_cls=cloudpickle.dumps(cls),
            connectors=connector_requests,
        )
        resp = stub.RegisterStream(req)
        return resp
