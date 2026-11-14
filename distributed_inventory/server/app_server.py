import json
import time

from ..proto import system_pb2, system_pb2_grpc


class InventoryAppServer(system_pb2_grpc.ClientServiceServicer):
    """Client-facing gRPC service: login, logout, order placement and inventory reads."""

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
                    self.raft_node.emit("node", f"order rejected: insufficient {item} for {qty}")
                    return system_pb2.StatusMessage(status="INSUFFICIENT_STOCK")

            command = {"action": "DECREMENT", "item": item, "qty": qty}
            if self.raft_node.replicate_command(command):
                target_index = len(self.raft_node.log) - 1
                self.raft_node.emit("node", f"order replicated: {qty} of {item} at log index {target_index}")
                timeout = time.time() + 2.0
                while time.time() < timeout:
                    if self.raft_node.commit_index >= target_index:
                        return system_pb2.StatusMessage(status="ORDER_SUCCESS")
                    time.sleep(0.05)
                self.raft_node.emit("node", f"order at log index {target_index} not committed within 2s")
                return system_pb2.StatusMessage(status="CONSENSUS_TIMEOUT")

        return system_pb2.StatusMessage(status="UNKNOWN_REQUEST")

    def get(self, request, context):
        if request.type == "inventory_levels":
            items = [system_pb2.GetResponse.Item(id=k, data=str(v)) for k, v in self.raft_node.inventory.items()]
            return system_pb2.GetResponse(status="SUCCESS", items=items)

        elif request.type in ["demand_prediction", "reorder_suggestions", "inventory_analytics"]:
            # Mock historical data for prediction; otherwise use current stock
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
