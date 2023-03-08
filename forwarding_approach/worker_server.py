__all__ = ["WorkerServicer", "start_worker_server"]

import grpc.aio

from forwarding_approach.common import ServerInfo
from service_pb2 import FooRequest, FooResponse
from service_pb2_grpc import ServiceServicer, add_ServiceServicer_to_server


class WorkerServicer(ServiceServicer):

    async def FooMethod(self, request: FooRequest, context: grpc.aio.ServicerContext) -> FooResponse:
        return FooResponse(request_id=request.request_id)


async def start_worker_server(
    host: str = "localhost",
    port: int = 0,
) -> ServerInfo:
    server = grpc.aio.server()
    servicer = WorkerServicer()
    add_ServiceServicer_to_server(servicer, server)
    port = server.add_insecure_port(f"{host}:{port}")
    await server.start()

    return ServerInfo(server=server, servicer=servicer, address=f"{host}:{port}")
