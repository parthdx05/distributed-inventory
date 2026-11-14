import json
from dataclasses import dataclass
from typing import Any, Iterator, Sequence

import grpc

from ..config import DEFAULT_CLUSTER, INSIGHT_FEATURES
from ..proto import system_pb2, system_pb2_grpc


@dataclass(frozen=True)
class Step:
    """One event from the client walkthrough.

    kind: "login", "info", "warning", "error", "order", "inventory",
          "insight_start", "insight", or "logout"
    message: human-readable text
    data: structured payload (order status, {item: quantity}, insight feature name, ...)
    """
    kind: str
    message: str = ""
    data: Any = None


def stub_for(address):
    """Client-facing stub for one node. Caller owns the channel lifetime."""
    channel = grpc.insecure_channel(address)
    return system_pb2_grpc.ClientServiceStub(channel)


def walkthrough(nodes: Sequence[str] = DEFAULT_CLUSTER, username="admin", password="password",
                item="ItemA", qty=10, insight_timeout=60) -> Iterator[Step]:
    """Run the client walkthrough, yielding a Step for each thing that happens.

    1. Log in via the first node.
    2. Place an order, trying nodes in order until one is the leader.
    3. Read inventory from the leader.
    4. Request each AI insight from the leader.
    5. Log out.
    """
    first = nodes[0]
    try:
        login = stub_for(first).login(system_pb2.LoginRequest(username=username, password=password), timeout=3)
    except grpc.RpcError as exc:
        yield Step("error", f"Login via {first} failed: {exc.code()}")
        return

    yield Step("login", f"Login Status via {first}: {login.status}", data=login.status)
    token = login.token

    # Order: try each node until one accepts it as leader
    order_data = json.dumps({"item": item, "qty": qty})
    leader_stub = None
    for node in nodes:
        yield Step("info", f"Attempting order via {node}...")
        stub = stub_for(node)
        try:
            res = stub.post(system_pb2.PostRequest(token=token, type="order", data=order_data), timeout=3)
        except grpc.RpcError:
            yield Step("warning", f"Node {node} is unreachable.")
            continue

        if res.status == "REDIRECT_TO_LEADER":
            yield Step("warning", f"Node {node} is a follower. Redirecting...")
            continue

        yield Step("order", f"Order Status from Leader {node}: {res.status}", data=res.status)
        leader_stub = stub
        break

    if leader_stub is None:
        yield Step("error", "Failed to find a cluster leader.")
        return

    # Read inventory from the leader
    try:
        inv = leader_stub.get(system_pb2.GetRequest(token=token, type="inventory_levels"), timeout=5)
    except grpc.RpcError as exc:
        yield Step("error", f"Inventory read failed: {exc.code()}")
        return
    yield Step("inventory", "Verified Inventory State", data={i.id: i.data for i in inv.items})

    # AI insights
    for feature in INSIGHT_FEATURES:
        yield Step("insight_start", f"Requesting AI Insight: {feature}", data=feature)
        try:
            res = leader_stub.get(system_pb2.GetRequest(token=token, type=feature), timeout=insight_timeout)
        except grpc.RpcError as exc:
            yield Step("error", f"AI insight {feature} failed: {exc.code()}", data=feature)
            continue
        yield Step("insight", "\n".join(i.data for i in res.items), data=feature)

    # Logout
    try:
        res = leader_stub.logout(system_pb2.TokenMessage(token=token), timeout=3)
        yield Step("logout", f"Logout Status: {res.status}", data=res.status)
    except grpc.RpcError as exc:
        yield Step("error", f"Logout failed: {exc.code()}")
