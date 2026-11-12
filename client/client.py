import sys
import grpc
import json

sys.path.append('../proto')
import system_pb2
import system_pb2_grpc

CLUSTER_NODES = ['localhost:50052', 'localhost:50053', 'localhost:50054']

def execute_order(token):
    order_data = json.dumps({"item": "ItemA", "qty": 10})
    
    for node in CLUSTER_NODES:
        try:
            channel = grpc.insecure_channel(node)
            stub = system_pb2_grpc.ClientServiceStub(channel)
            
            print(f"Attempting order via {node}...")
            res = stub.post(system_pb2.PostRequest(token=token, type="order", data=order_data), timeout=3)
            
            if res.status == "REDIRECT_TO_LEADER":
                print(f" - Node {node} is a follower. Redirecting...")
                continue
                
            print(f" - Order Status from Leader {node}: {res.status}")
            return node, stub
            
        except grpc.RpcError:
            print(f" - Node {node} is unreachable.")
            continue
            
    print("Failed to find a cluster leader.")
    return None, None

def run():
    active_node = CLUSTER_NODES[0]
    channel = grpc.insecure_channel(active_node)
    stub = system_pb2_grpc.ClientServiceStub(channel)
    
    login_res = stub.login(system_pb2.LoginRequest(username="admin", password="password"))
    token = login_res.token
    print(f"Login Status via {active_node}: {login_res.status}\n")

    leader_node, leader_stub = execute_order(token)
    
    if leader_stub:
        get_res = leader_stub.get(system_pb2.GetRequest(token=token, type="inventory_levels"))
        print("\nVerified Inventory State:")
        for item in get_res.items:
            print(f" - {item.id}: {item.data}")
            
        # Option B Specific LLM Integrations[cite: 2]
        llm_features = ["demand_prediction", "reorder_suggestions", "inventory_analytics"]
        
        for feature in llm_features:
            print(f"\n--- Requesting AI Insight: {feature} ---")
            res = leader_stub.get(system_pb2.GetRequest(token=token, type=feature), timeout=60)
            for item in res.items:
                print(f"{item.data}")

        # Call logout at the end of the script
        print("\n--- Ending Session ---")
        logout_res = leader_stub.logout(system_pb2.TokenMessage(token=token))
        print(f"Logout Status: {logout_res.status}")

if __name__ == '__main__':
    run()