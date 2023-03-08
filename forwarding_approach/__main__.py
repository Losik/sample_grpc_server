import asyncio
import logging
import random
from typing import NamedTuple, Awaitable

import grpc.aio

from service_pb2 import FooRequest, FooResponse
from service_pb2_grpc import ServiceServicer, add_ServiceServicer_to_server, ServiceStub

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


class ServerInfo(NamedTuple):
    server: grpc.aio.Server
    servicer: ServiceServicer
    address: str


class RealServicer(ServiceServicer):

    async def FooMethod(self, request: FooRequest, context: grpc.aio.ServicerContext) -> FooResponse:
        return FooResponse(request_id=request.request_id)


class BalancerServicer(ServiceServicer):

    def __init__(self, addresses: list[str]):
        self._channels = [grpc.aio.insecure_channel(address) for address in addresses]
        self._stubs = [ServiceStub(channel=channel) for channel in self._channels]

    async def close(self):
        for channel in self._channels:
            await channel.__aexit__(None, None, None)

    async def FooMethod(self, request: FooRequest, context: grpc.aio.ServicerContext) -> FooResponse:
        # Rude balancing
        stub = random.choice(self._stubs)
        return await stub.FooMethod(request)


async def start_server(servicer: ServiceServicer, host: str = "localhost", port: int = 0) -> ServerInfo:
    server = grpc.aio.server()
    add_ServiceServicer_to_server(servicer, server)
    port = server.add_insecure_port(f"{host}:{port}")
    await server.start()

    return ServerInfo(server=server, servicer=servicer, address=f"{host}:{port}")


def start_real_server(host: str = "localhost", port: int = 0) -> Awaitable[ServerInfo]:
    return start_server(servicer=RealServicer(), host=host, port=port)


def start_balancer_server(addresses: list[str], host: str = "localhost", port: int = 0) -> Awaitable[ServerInfo]:
    servicer = BalancerServicer(addresses=addresses)
    return start_server(servicer=servicer, host=host, port=port)


async def start_client(client_id: int, address: str = "localhost:6565"):

    async with grpc.aio.insecure_channel(address) as channel:
        stub = ServiceStub(channel=channel)
        for i in range(10):
            request = FooRequest(request_id=f"{client_id}:{i}")
            logging.info(f"Request {request.request_id}")
            response = await stub.FooMethod(request)
            logging.info(f"Response {response.request_id}")


async def main(real_servers_count: int, host: str = "localhost", port: int = 0):

    # Starting GRPC server
    start_real_server_tasks = [start_real_server(host, port) for _ in range(real_servers_count)]
    real_server_infos: list[ServerInfo] = await asyncio.gather(*start_real_server_tasks)
    real_server_addresses = [server_info.address for server_info in real_server_infos]
    balancer_servicer_info = await start_balancer_server(real_server_addresses)

    # Running clients
    await asyncio.gather(
        *[
            asyncio.create_task(start_client(client_id, balancer_servicer_info.address))
            for client_id in range(1)
        ]
    )

    # Shutting down GRPC server
    await balancer_servicer_info.server.stop(grace=1)
    await balancer_servicer_info.servicer.close()

    for real_service_info in real_server_infos:
        await real_service_info.server.stop(grace=1)



if __name__ == "__main__":
    asyncio.run(main(real_servers_count=3))
