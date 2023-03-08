from typing import NamedTuple

import grpc.aio

from forwarding_approach.service_pb2_grpc import ServiceServicer


class ServerInfo(NamedTuple):
    server: grpc.aio.Server
    servicer: ServiceServicer
    address: str
