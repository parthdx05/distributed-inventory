import sys
import grpc
import json
import threading
import time

sys.path.append('../proto')
import system_pb2
import system_pb2_grpc

def place_order(stub, token, thread_id):
    order_data = json.dumps({"item": "ItemA", "qty": 10})
    res = stub.post(system_pb2.PostRequest(token=token, type="order", data=order_data), timeout=5)
    print(f"Thread {thread_id} Result: {res.status}")

def run_stress_test():
    # Assume 50052 is the active leader for the test
    channel = grpc.insecure_channel('localhost:50052')
    stub = system_pb2_grpc.ClientServiceStub(channel)
    
    login_res = stub.login(system_pb2.LoginRequest(username="admin", password="password"))
    token = login_res.token

    print("Initial Inventory:")
    get_res = stub.get(system_pb2.GetRequest(token=token, type="inventory_levels"))
    for item in get_res.items:
        print(f" - {item.id}: {item.data}")
        
    print("\nStarting Stress Test with 15 concurrent orders (150 total qty attempt for 100 stock)...")
    threads = []
    for i in range(15):
        t = threading.Thread(target=place_order, args=(stub, token, i))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    print("\nFinal Inventory (Should be 0, never negative):")
    final_res = stub.get(system_pb2.GetRequest(token=token, type="inventory_levels"))
    for item in final_res.items:
        print(f" - {item.id}: {item.data}")

if __name__ == '__main__':
    run_stress_test()