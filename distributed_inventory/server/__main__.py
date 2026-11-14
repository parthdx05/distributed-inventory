"""Run one inventory node from the command line.

    python -m distributed_inventory.server --port 50052
"""

import argparse

from ..config import DEFAULT_CLUSTER, DEFAULT_LLM_ADDRESS
from .node import InventoryNode


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run one inventory application server and Raft node.")
    parser.add_argument("--host", default="localhost", help="address peers and clients use to reach this node")
    parser.add_argument("--port", type=int, default=50052)
    parser.add_argument("--peers", nargs="*", default=list(DEFAULT_CLUSTER), help="peer host:port addresses")
    parser.add_argument("--llm", default=DEFAULT_LLM_ADDRESS, help="LLM service host:port")
    parser.add_argument("--state-dir", default=None, help="directory for raft_state_*.json (default: current directory)")
    args = parser.parse_args(argv)

    node = InventoryNode(args.host, args.port, args.peers, llm_address=args.llm, state_dir=args.state_dir)
    node.start()
    print(f"Application Server & Raft Node running on {node.address}...")
    try:
        node.wait()
    except KeyboardInterrupt:
        node.stop()


if __name__ == "__main__":
    main()
