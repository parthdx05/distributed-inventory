"""Run the client walkthrough from the command line.

    python -m distributed_inventory.client --nodes localhost:50052 localhost:50053 localhost:50054
"""

import argparse

from ..config import DEFAULT_CLUSTER
from .cluster_client import walkthrough


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run the inventory client walkthrough.")
    parser.add_argument("--nodes", nargs="+", default=list(DEFAULT_CLUSTER), help="cluster host:port addresses")
    parser.add_argument("--username", default="admin")
    parser.add_argument("--password", default="password")
    parser.add_argument("--item", default="ItemA")
    parser.add_argument("--qty", type=int, default=10)
    args = parser.parse_args(argv)

    for step in walkthrough(args.nodes, args.username, args.password, args.item, args.qty):
        if step.kind == "inventory":
            print(f"\n{step.message}:")
            for item_id, quantity in step.data.items():
                print(f" - {item_id}: {quantity}")
        elif step.kind == "insight_start":
            print(f"\n--- {step.message} ---")
        elif step.kind == "logout":
            print("\n--- Ending Session ---")
            print(step.message)
        else:
            print(step.message)


if __name__ == "__main__":
    main()
