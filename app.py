"""Streamlit front end for the distributed inventory.

    streamlit run app.py

Server mode starts inventory nodes and shows their state, node log and leader
election log, refreshed every second. Client mode runs the client walkthrough
against a cluster and shows each step as it happens.
"""

import pandas as pd
import streamlit as st

from distributed_inventory.client import Step, walkthrough
from distributed_inventory.config import DEFAULT_CLUSTER, DEFAULT_LLM_ADDRESS
from distributed_inventory.server import InventoryNode

DEFAULT_CLUSTER_TEXT = ", ".join(DEFAULT_CLUSTER)
DEFAULT_CLUSTER_DF = pd.DataFrame(
    [{"host": host, "port": int(port)} for host, port in (addr.rsplit(":", 1) for addr in DEFAULT_CLUSTER)]
)
LOG_LINES_SHOWN = 200


@st.cache_resource
def running_nodes():
    """Nodes started from this app, keyed by address.

    Cached at process level, so a node keeps running across reruns and browser
    reloads. Restarting the Streamlit process stops all nodes.
    """
    return {}


def parse_addresses(text):
    return [part.strip() for part in text.replace("\n", ",").split(",") if part.strip()]


def rows_to_addresses(df):
    """Turn host/port table rows into host:port strings, skipping incomplete rows."""
    addresses = []
    for host, port in zip(df["host"], df["port"]):
        if not isinstance(host, str) or not host.strip() or pd.isna(port):
            continue
        addresses.append(f"{host.strip()}:{int(port)}")
    return addresses


# Server

def server_panel():
    nodes = running_nodes()

    st.subheader("Create node")
    host = st.text_input(
        "IP address or hostname",
        value="localhost",
        help="Address that peers and clients use to reach this node.",
    ).strip()
    port = int(st.number_input("Port", min_value=1, max_value=65535, value=50052, step=1))
    peers_text = st.text_input(
        "Peer nodes (host:port, comma-separated)",
        value=DEFAULT_CLUSTER_TEXT,
        help="This node is removed from the list if it appears.",
    )
    llm_address = st.text_input("LLM service address (host:port)", value=DEFAULT_LLM_ADDRESS)

    if st.button("Start node", type="primary", disabled=not host):
        address = f"{host}:{port}"
        existing = nodes.get(address)
        if existing is not None and existing.running:
            st.warning(f"A node is already running at {address}.")
        else:
            try:
                node = InventoryNode(host, port, parse_addresses(peers_text), llm_address=llm_address)
                node.start()
                nodes[address] = node
                st.success(f"Node started at {address}.")
            except Exception as exc:  # e.g. port already in use
                st.error(f"Could not start node at {address}: {exc}")

    st.divider()
    st.subheader("Existing nodes")
    addresses = [addr for addr, node in nodes.items() if node.running]
    if not addresses:
        st.info("No nodes running. Create one above.")
        return

    selected = st.selectbox("Node", addresses, key="node_select")
    node = nodes[selected]
    if st.button("Stop node"):
        node.stop()
        nodes.pop(selected, None)
        st.rerun()
    node_view(node)


@st.fragment(run_every=1)
def node_view(node):
    raft = node.raft
    with raft.lock:
        inventory = dict(raft.inventory)
        role, term, commit = raft.state, raft.current_term, raft.commit_index

    st.caption(f"Peers: {', '.join(node.peers) if node.peers else 'none'}")
    col_role, col_term, col_commit = st.columns(3)
    col_role.metric("Role", role)
    col_term.metric("Term", term)
    col_commit.metric("Commit index", commit)

    st.dataframe(
        [{"item": k, "quantity": v} for k, v in inventory.items()],
        hide_index=True,
    )

    col_node, col_election = st.columns(2)
    with col_node:
        st.markdown("**Node log**")
        log_box([f"{stamp}  [{cat}] {msg}" for stamp, cat, msg in raft.events()])
    with col_election:
        st.markdown("**Leader election log**")
        log_box([f"{stamp}  {msg}" for stamp, _, msg in raft.events("election")])


def log_box(lines):
    with st.container(height=320):
        st.code("\n".join(lines[-LOG_LINES_SHOWN:]) or "(no events yet)", language=None)


# Client

def client_panel():
    st.subheader("Cluster nodes")
    st.caption("Add, edit or delete nodes. Rows with an empty IP are ignored.")
    cluster_df = st.data_editor(
        DEFAULT_CLUSTER_DF,
        num_rows="dynamic",
        hide_index=True,
        key="cluster_editor",
        column_config={
            "host": st.column_config.TextColumn("IP address or hostname", required=True),
            "port": st.column_config.NumberColumn("Port", min_value=1, max_value=65535, step=1, required=True),
        },
    )
    addresses = rows_to_addresses(cluster_df)

    col_user, col_pass = st.columns(2)
    username = col_user.text_input("Username", value="admin")
    password = col_pass.text_input("Password", value="password", type="password")
    col_item, col_qty = st.columns(2)
    item = col_item.text_input("Item", value="ItemA")
    qty = col_qty.number_input("Quantity", min_value=1, value=10, step=1)

    if st.button("Run walkthrough", type="primary", disabled=not addresses):
        render_walkthrough(addresses, username, password, item, int(qty))


def render_walkthrough(nodes, username, password, item, qty):
    failed = False
    with st.status("Running client walkthrough...", expanded=True) as status:
        for step in walkthrough(nodes, username=username, password=password, item=item, qty=qty):
            render_step(step)
            failed = failed or step.kind == "error"
        if failed:
            status.update(label="Walkthrough finished with errors", state="error")
        else:
            status.update(label="Walkthrough complete", state="complete")


def render_step(step: Step):
    if step.kind == "error":
        st.error(step.message)
    elif step.kind == "warning":
        st.warning(step.message)
    elif step.kind == "order":
        if step.data == "ORDER_SUCCESS":
            st.success(step.message)
        else:
            st.warning(step.message)
    elif step.kind == "inventory":
        st.subheader(step.message)
        st.dataframe(
            [{"item": k, "quantity": _as_number(v)} for k, v in step.data.items()],
            hide_index=True,
        )
    elif step.kind == "insight_start":
        st.subheader(step.message)
    elif step.kind == "insight":
        st.text(step.message)
    elif step.kind == "logout":
        st.info(step.message)
    else:
        st.write(step.message)


def _as_number(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


# Page

st.set_page_config(page_title="Distributed Inventory", layout="wide")
st.title("Distributed Inventory")

mode = st.radio("Mode", ["Server", "Client"], horizontal=True)
if mode == "Server":
    server_panel()
else:
    client_panel()
