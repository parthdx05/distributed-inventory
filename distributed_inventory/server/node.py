from concurrent import futures

import grpc

from ..config import DEFAULT_INVENTORY, DEFAULT_LLM_ADDRESS
from ..proto import system_pb2_grpc
from .app_server import InventoryAppServer
from .raft_node import RaftNode


class InventoryNode:
    """One application server plus its Raft node, served over gRPC.

    `host` and `port` form the address that peers and clients use. The gRPC
    server binds to all interfaces on `port`.
    """

    def __init__(self, host, port, peers, llm_address=DEFAULT_LLM_ADDRESS,
                 inventory=None, state_dir=None, max_workers=10):
        self.host = host.strip()
        self.port = int(port)
        self.address = f"{self.host}:{self.port}"
        # A node never lists itself as a peer.
        self.peers = [p for p in peers if p != self.address]
        self.llm_address = llm_address
        self._inventory_seed = dict(inventory or DEFAULT_INVENTORY)
        self.state_dir = state_dir
        self.max_workers = max_workers

        self.raft = None
        self._grpc_server = None
        self._llm_channel = None

    @property
    def running(self):
        return self._grpc_server is not None

    @property
    def inventory(self):
        return self.raft.inventory if self.raft else dict(self._inventory_seed)

    def start(self):
        if self.running:
            raise RuntimeError(f"Node {self.address} is already running")

        self.raft = RaftNode(self.address, self.peers, dict(self._inventory_seed), state_dir=self.state_dir)
        self._llm_channel = grpc.insecure_channel(self.llm_address)
        llm_stub = system_pb2_grpc.LLMServiceStub(self._llm_channel)

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=self.max_workers))
        system_pb2_grpc.add_ClientServiceServicer_to_server(InventoryAppServer(self.raft, llm_stub), server)
        system_pb2_grpc.add_RaftServiceServicer_to_server(self.raft, server)
        server.add_insecure_port(f"[::]:{self.port}")
        server.start()
        self._grpc_server = server

    def wait(self):
        """Block until the gRPC server terminates."""
        if self._grpc_server:
            self._grpc_server.wait_for_termination()

    def stop(self, grace=0):
        if not self.running:
            return
        self._grpc_server.stop(grace).wait()
        self._grpc_server = None
        self.raft.stop()
        self._llm_channel.close()
        self._llm_channel = None
