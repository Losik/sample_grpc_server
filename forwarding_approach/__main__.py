import asyncio
import logging

import grpc.aio

from forwarding_approach.balancer_server import start_balancer_server
from forwarding_approach.common import ServerInfo
from forwarding_approach.worker_server import start_worker_server
from service_pb2 import FooRequest
from service_pb2_grpc import ServiceStub

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


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
    start_worker_server_tasks = [start_worker_server(host, port) for _ in range(real_servers_count)]
    worker_server_infos: list[ServerInfo] = await asyncio.gather(*start_worker_server_tasks)
    worker_server_addresses = [server_info.address for server_info in worker_server_infos]
    balancer_servicer_info = await start_balancer_server(worker_server_addresses)

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

    for real_service_info in worker_server_infos:
        await real_service_info.server.stop(grace=1)


if __name__ == "__main__":
    asyncio.run(main(real_servers_count=3))
