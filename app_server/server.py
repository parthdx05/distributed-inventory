import sys
import grpc
import threading
import json
import time
from concurrent import futures

sys.path.append('../proto')
import system_pb2
import system_pb2_grpc
from raft_node import RaftNode

class InventoryAppServer(system_pb2_grpc.ClientServiceServicer):
    def __init__(self, raft_node, llm_stub):
        self.raft_node = raft_node
        self.llm_stub = llm_stub

    def login(self, request, context):
        if request.username and request.password:
            return system_pb2.LoginResponse(status="SUCCESS", token="mock_token_123")
        return system_pb2.LoginResponse(status="FAILED", token="")

    def logout(self, request, context):
        # In a fully authenticated system, you would invalidate the token here.
        if request.token:
            return system_pb2.StatusMessage(status="LOGOUT_SUCCESS")
        return system_pb2.StatusMessage(status="LOGOUT_FAILED")

    def post(self, request, context):
        if request.type == "order":
            if self.raft_node.state != "LEADER":
                return system_pb2.StatusMessage(status="REDIRECT_TO_LEADER")
                
            order_data = json.loads(request.data)
            item = order_data.get("item")
            qty = order_data.get("qty")
            
            with self.raft_node.lock:
                if self.raft_node.inventory.get(item, 0) < qty:
                    return system_pb2.StatusMessage(status="INSUFFICIENT_STOCK")
            
            command = {"action": "DECREMENT", "item": item, "qty": qty}
            if self.raft_node.replicate_command(command):
                target_index = len(self.raft_node.log) - 1
                timeout = time.time() + 2.0
                while time.time() < timeout:
                    if self.raft_node.commit_index >= target_index:
                        return system_pb2.StatusMessage(status="ORDER_SUCCESS")
                    time.sleep(0.05)
                return system_pb2.StatusMessage(status="CONSENSUS_TIMEOUT")
                
        return system_pb2.StatusMessage(status="UNKNOWN_REQUEST")

    def get(self, request, context):
        if request.type == "inventory_levels":
            items = [system_pb2.GetResponse.Item(id=k, data=str(v)) for k, v in self.raft_node.inventory.items()]
            return system_pb2.GetResponse(status="SUCCESS", items=items)
            
        elif request.type in ["demand_prediction", "reorder_suggestions", "inventory_analytics"]:
            # Provide mock historical data for prediction, otherwise use current stock[cite: 2]
            if request.type == "demand_prediction":
                payload = json.dumps({"ItemA_last_month": 450, "ItemB_last_month": 120})
            else:
                payload = json.dumps(self.raft_node.inventory)
                
            llm_req = system_pb2.LLMQuery(
                request_id=f"req_{request.type}", 
                query=payload, 
                context=request.type
            )
            llm_response = self.llm_stub.getLLMAnswer(llm_req)
            items = [system_pb2.GetResponse.Item(id=request.type, data=llm_response.answer)]
            return system_pb2.GetResponse(status="SUCCESS", items=items)

def serve():
    port = sys.argv[1] if len(sys.argv) > 1 else '50052'
    node_address = f'localhost:{port}'
    
    all_nodes = ['localhost:50052', 'localhost:50053', 'localhost:50054']
    peers = [p for p in all_nodes if p != node_address]
    
    inventory = {"ItemA": 100, "ItemB": 50}
    raft_node = RaftNode(node_address, peers, inventory)
    
    llm_channel = grpc.insecure_channel('localhost:50051')
    llm_stub = system_pb2_grpc.LLMServiceStub(llm_channel)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    system_pb2_grpc.add_ClientServiceServicer_to_server(InventoryAppServer(raft_node, llm_stub), server)
    system_pb2_grpc.add_RaftServiceServicer_to_server(raft_node, server)
    
    server.add_insecure_port(f'[::]:{port}')
    print(f"Application Server & Raft Node running on port {port}...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()