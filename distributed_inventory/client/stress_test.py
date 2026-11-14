"""Send concurrent ItemA orders directly to one node.

It assumes that node is the current leader and does not follow redirects.

    python -m distributed_inventory.client.stress_test --node localhost:50052
"""

import argparse
import json
import threading

from ..proto import system_pb2
from .cluster_client import stub_for


def place_order(stub, token, thread_id, item, qty, results):
    order_data = json.dumps({"item": item, "qty": qty})
    res = stub.post(system_pb2.PostRequest(token=token, type="order", data=order_data), timeout=5)
    results[thread_id] = res.status
    print(f"Thread {thread_id} Result: {res.status}")


def run_stress_test(node="localhost:50052", threads=15, item="ItemA", qty=10):
    """Returns (final inventory dict, list of per-thread statuses)."""
    stub = stub_for(node)
    token = stub.login(system_pb2.LoginRequest(username="admin", password="password")).token

    print("Initial Inventory:")
    _print_inventory(stub, token)

    print(f"\nStarting Stress Test with {threads} concurrent orders "
          f"({threads * qty} total qty attempt)...")
    results = [None] * threads
    workers = [threading.Thread(target=place_order, args=(stub, token, i, item, qty, results))
               for i in range(threads)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()

    print("\nFinal Inventory:")
    final = _print_inventory(stub, token)
    return final, results


def _print_inventory(stub, token):
    res = stub.get(system_pb2.GetRequest(token=token, type="inventory_levels"))
    inventory = {}
    for item in res.items:
        print(f" - {item.id}: {item.data}")
        inventory[item.id] = item.data
    return inventory


def main(argv=None):
    parser = argparse.ArgumentParser(description="Concurrent order stress test against one node.")
    parser.add_argument("--node", default="localhost:50052")
    parser.add_argument("--threads", type=int, default=15)
    parser.add_argument("--qty", type=int, default=10)
    args = parser.parse_args(argv)
    run_stress_test(args.node, args.threads, qty=args.qty)


if __name__ == "__main__":
    main()
