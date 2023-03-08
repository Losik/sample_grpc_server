__all__ = ["BalancerServicer", "start_balancer_server"]

import random
from functools import wraps

import grpc.aio

from forwarding_approach.common import ServerInfo
from service_pb2 import FooResponse
from service_pb2_grpc import ServiceServicer, add_ServiceServicer_to_server, ServiceStub


def identity_func(value):
    return value


def make_forwarding_call(grpc_call):

    @wraps(grpc_call)
    def forwarding(method, *args, **kwargs):
        return grpc_call(
            method=method,
            request_serializer=identity_func,
            response_deserializer=identity_func,
        )

    return forwarding


def create_forwarding_channel(target=None, options=None, credentials=None, compression=None, interceptors=None):

    channel = grpc.aio._channel.Channel(
        target=target,
        options=() if options is None else options,
        credentials=credentials,
        compression=compression,
        interceptors=interceptors,
    )

    channel.unary_unary = make_forwarding_call(channel.unary_unary)
    channel.unary_stream = make_forwarding_call(channel.unary_stream)
    channel.stream_unary = make_forwarding_call(channel.stream_unary)
    channel.stream_stream = make_forwarding_call(channel.stream_stream)

    return channel


class BalancerServicer(ServiceServicer):

    def __init__(self, addresses: list[str]):
        self._channels = [create_forwarding_channel(address) for address in addresses]
        self._stubs = [ServiceStub(channel=channel) for channel in self._channels]

    async def close(self):
        for channel in self._channels:
            await channel.__aexit__(None, None, None)

    async def FooMethod(self, request: str, context: grpc.aio.ServicerContext) -> FooResponse:
        # Rude balancing
        stub = random.choice(self._stubs)
        return await stub.FooMethod(request)


async def start_balancer_server(
    addresses: list[str],
    host: str = "localhost",
    port: int = 0,
) -> ServerInfo:
    server = grpc.aio.server()
    servicer = BalancerServicer(addresses=addresses)

    rpc_method_handlers = {
        'FooMethod': grpc.unary_unary_rpc_method_handler(
            servicer.FooMethod,
            # Do not serialize/deserialize. Just forward
            request_deserializer=lambda x: x,
            response_serializer=lambda x: x,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        service='Service',
        method_handlers=rpc_method_handlers,
    )
    server.add_generic_rpc_handlers((generic_handler,))

    add_ServiceServicer_to_server(servicer, server)
    port = server.add_insecure_port(f"{host}:{port}")
    await server.start()

    return ServerInfo(server=server, servicer=servicer, address=f"{host}:{port}")