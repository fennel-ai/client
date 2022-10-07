# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import fennel.gen.aggregate_pb2 as aggregate__pb2
import fennel.gen.feature_pb2 as feature__pb2
import fennel.gen.status_pb2 as status__pb2
import fennel.gen.stream_pb2 as stream__pb2


class FennelFeatureStoreStub(object):
    """
    The generated code needs to be hand modified to add the following
    import stream_pb2 as stream__pb2 to import fennel.gen.stream_pb2 as stream__pb2
    More info: https://github.com/protocolbuffers/protobuf/issues/1491 & https://github.com/protocolbuffers/protobuf/issues/881

    Use the following command from client ( root ) directory to generated the required files -
    https://gist.github.com/aditya-nambiar/87c8b55fe509c1c1cd06d212b8a8ded1
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.RegisterStream = channel.unary_unary(
                '/fennel.proto.FennelFeatureStore/RegisterStream',
                request_serializer=stream__pb2.CreateStreamRequest.SerializeToString,
                response_deserializer=status__pb2.Status.FromString,
                )
        self.RegisterAggregate = channel.unary_unary(
                '/fennel.proto.FennelFeatureStore/RegisterAggregate',
                request_serializer=aggregate__pb2.CreateAggregateRequest.SerializeToString,
                response_deserializer=status__pb2.Status.FromString,
                )
        self.RegisterFeature = channel.unary_unary(
                '/fennel.proto.FennelFeatureStore/RegisterFeature',
                request_serializer=feature__pb2.CreateFeatureRequest.SerializeToString,
                response_deserializer=status__pb2.Status.FromString,
                )
        self.ExtractFeatures = channel.unary_unary(
                '/fennel.proto.FennelFeatureStore/ExtractFeatures',
                request_serializer=feature__pb2.ExtractFeaturesRequest.SerializeToString,
                response_deserializer=feature__pb2.ExtractFeaturesResponse.FromString,
                )


class FennelFeatureStoreServicer(object):
    """
    The generated code needs to be hand modified to add the following
    import stream_pb2 as stream__pb2 to import fennel.gen.stream_pb2 as stream__pb2
    More info: https://github.com/protocolbuffers/protobuf/issues/1491 & https://github.com/protocolbuffers/protobuf/issues/881

    Use the following command from client ( root ) directory to generated the required files -
    https://gist.github.com/aditya-nambiar/87c8b55fe509c1c1cd06d212b8a8ded1
    """

    def RegisterStream(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RegisterAggregate(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RegisterFeature(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ExtractFeatures(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_FennelFeatureStoreServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'RegisterStream': grpc.unary_unary_rpc_method_handler(
                    servicer.RegisterStream,
                    request_deserializer=stream__pb2.CreateStreamRequest.FromString,
                    response_serializer=status__pb2.Status.SerializeToString,
            ),
            'RegisterAggregate': grpc.unary_unary_rpc_method_handler(
                    servicer.RegisterAggregate,
                    request_deserializer=aggregate__pb2.CreateAggregateRequest.FromString,
                    response_serializer=status__pb2.Status.SerializeToString,
            ),
            'RegisterFeature': grpc.unary_unary_rpc_method_handler(
                    servicer.RegisterFeature,
                    request_deserializer=feature__pb2.CreateFeatureRequest.FromString,
                    response_serializer=status__pb2.Status.SerializeToString,
            ),
            'ExtractFeatures': grpc.unary_unary_rpc_method_handler(
                    servicer.ExtractFeatures,
                    request_deserializer=feature__pb2.ExtractFeaturesRequest.FromString,
                    response_serializer=feature__pb2.ExtractFeaturesResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'fennel.proto.FennelFeatureStore', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class FennelFeatureStore(object):
    """
    The generated code needs to be hand modified to add the following
    import stream_pb2 as stream__pb2 to import fennel.gen.stream_pb2 as stream__pb2
    More info: https://github.com/protocolbuffers/protobuf/issues/1491 & https://github.com/protocolbuffers/protobuf/issues/881

    Use the following command from client ( root ) directory to generated the required files -
    https://gist.github.com/aditya-nambiar/87c8b55fe509c1c1cd06d212b8a8ded1
    """

    @staticmethod
    def RegisterStream(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/fennel.proto.FennelFeatureStore/RegisterStream',
            stream__pb2.CreateStreamRequest.SerializeToString,
            status__pb2.Status.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def RegisterAggregate(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/fennel.proto.FennelFeatureStore/RegisterAggregate',
            aggregate__pb2.CreateAggregateRequest.SerializeToString,
            status__pb2.Status.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def RegisterFeature(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/fennel.proto.FennelFeatureStore/RegisterFeature',
            feature__pb2.CreateFeatureRequest.SerializeToString,
            status__pb2.Status.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ExtractFeatures(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/fennel.proto.FennelFeatureStore/ExtractFeatures',
            feature__pb2.ExtractFeaturesRequest.SerializeToString,
            feature__pb2.ExtractFeaturesResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
