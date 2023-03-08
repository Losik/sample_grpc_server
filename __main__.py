import asyncio
import logging
from concurrent.futures import ProcessPoolExecutor

import grpc.aio
from grpc.aio import ServicerContext

from service_pb2 import FooRequest, FooResponse
from service_pb2_grpc import ServiceServicer, add_ServiceServicer_to_server, ServiceStub

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


class Servicer(ServiceServicer):

    def __init__(self):
        self._executor = ProcessPoolExecutor(max_workers=5)

    @staticmethod
    def foo(request: FooRequest):
        return FooResponse(request_id=request.request_id)

    async def FooMethod(self, request: FooRequest, context: ServicerContext) -> FooResponse:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self.foo, request)


async def start_client(client_id: int, address: str = "localhost:6565"):

    async with grpc.aio.insecure_channel(address) as channel:
        stub = ServiceStub(channel=channel)
        for i in range(10):
            request = FooRequest(request_id=f"{client_id}:{i}")
            logging.info(f"Request {request.request_id}")
            response = await stub.FooMethod(request)
            logging.info(f"Response {response.request_id}")


async def main(host: str = "localhost", port: int = 0):

    # Setting up GRPC
    server = grpc.aio.server()
    add_ServiceServicer_to_server(Servicer(), server)
    port = server.add_insecure_port(f"{host}:{port}")
    address = f"{host}:{port}"
    await server.start()

    # Running clients
    await asyncio.gather(
        *[
            asyncio.create_task(start_client(client_id, address))
            for client_id in range(1)
        ]
    )

    # Shutting down GRPC server
    await server.stop(grace=1)


if __name__ == "__main__":
    asyncio.run(main())
