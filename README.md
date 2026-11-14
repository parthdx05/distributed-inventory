# Distributed Inventory

Advanced Operating Systems project under Dr. Asish Bera, Birla Institute of
Technology & Science, Pilani-Pilani Campus.

| Name | BITS ID | BITS email |
|---|---|---|
| Mitul Vashista | 2026H1030071 | H20260071@pilani.bits-pilani.ac.in |
| Ruchir Kumar | 2026H1030083 | H2026083@pilani.bits-pilani.ac.in |
| Parth | 2026H1120137P | H20260137@pilani.bits-pilani.ac.in |

A Python/gRPC prototype for a distributed inventory service. It combines a
three-node Raft-style cluster for replicating inventory orders with a separate
LLM service that produces inventory-related text insights. A Streamlit front
end can run inventory nodes and show their state, logs and leader elections.

The project is intended to demonstrate service boundaries, gRPC communication,
leader-based writes, log replication, and an AI-backed read path. It is not a
production inventory or consensus system; see [Current behavior and
limitations](#current-behavior-and-limitations).

## What it does

- Accepts client login, logout, inventory-read, and order requests over gRPC.
- Runs inventory nodes (default ports `50052`, `50053`, `50054`).
- Elects a leader and replicates order commands to peer nodes.
- Applies committed decrement commands to each node's in-memory inventory.
- Sends demand-prediction, reorder-suggestion, and inventory-analytics prompts
  to a separate gRPC service running TinyLlama locally.
- Provides a Streamlit app to start nodes, watch their node and election logs,
  and run the client walkthrough.

Each node starts with the same sample inventory:

| Item | Starting quantity |
|---|---:|
| `ItemA` | 100 |
| `ItemB` | 50 |

## Architecture

```text
 client (CLI or Streamlit)
       |
       | gRPC: login / post / get / logout
       v
 +-----------------------------------------------------+
 |             Three-node Raft cluster                 |
 |                                                     |
 |   +------------+  +------------+  +------------+   |
 |   | node       |  | node       |  | node       |   |
 |   | :50052     |  | :50053     |  | :50054     |   |
 |   +------------+  +------------+  +------------+   |
 |   Each node exchanges Raft RPCs with its peers.     |
 +---------------------------+-------------------------+
                             |
                             | LLMService.getLLMAnswer
                             | gRPC to localhost:50051
                             v
                    +---------------------+
                    | LLM service         |
                    | TinyLlama / CPU     |
                    |       :50051        |
                    +---------------------+
```

Each node process hosts both the client-facing `ClientService` and the
inter-node `RaftService`. The LLM service runs as a separate process.

## Request flow

### Placing an order

1. The client logs in through a node.
2. It sends an `order` request containing a JSON item name and quantity.
3. If the contacted node is not the leader, it returns
   `REDIRECT_TO_LEADER`. The client tries the next configured node.
4. The leader checks its local stock, appends a `DECREMENT` command to its
   Raft log, and sends log entries to peers using `appendEntries`.
5. Once the leader advances the command's commit index, the request returns
   `ORDER_SUCCESS`. A background state-machine thread applies committed
   decrements to inventory.
6. The client reads inventory from the leader and then logs out.

The order payload is JSON shaped like `{"item":"ItemA","qty":10}`. The public
gRPC messages and methods are defined in
[`distributed_inventory/proto/system.proto`](distributed_inventory/proto/system.proto).

### Reading inventory and requesting insights

- `inventory_levels` returns the node's current in-memory item quantities.
- `demand_prediction` sends sample historical data hard-coded in the
  application server to the LLM service.
- `reorder_suggestions` sends the current inventory JSON to the LLM service.
- `inventory_analytics` sends the current inventory JSON to the LLM service.

The LLM service builds a task-specific prompt, generates up to 150 new tokens,
and returns the generated text. The model runs on CPU.

## Repository layout

```text
app.py                             Streamlit front end (streamlit run app.py)
pyproject.toml                     package metadata and optional dependency groups
requirements.txt                   pinned-by-minimum dependencies for pip install -r
distributed_inventory/             importable Python package
  config.py                        cluster addresses, LLM address, default inventory
  proto/
    system.proto                   gRPC service and message definitions
    system_pb2.py                  generated protobuf messages
    system_pb2_grpc.py             generated gRPC client/server bindings
  server/
    node.py                        InventoryNode: starts and stops one node
    raft_node.py                   Raft RPCs, election/heartbeat threads, log, state machine, event log
    app_server.py                  client-facing ClientService
    __main__.py                    run one node from the command line
  client/
    cluster_client.py              walkthrough() generator used by the CLI and the Streamlit app
    stress_test.py                 concurrent orders against one node
    __main__.py                    run the walkthrough from the command line
  llm/
    llm_service.py                 TinyLlama-backed gRPC service
    __main__.py                    run the LLM service from the command line
```

## Setup

Python 3.10 or newer is required. The generated bindings need
`grpcio >= 1.84.0` and `protobuf >= 7.35.1`.

From the repository root:

```sh
python -m venv .venv
source .venv/bin/activate          # Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

`requirements.txt` includes `transformers` and `torch` for the LLM service,
which is a large install. If you only need the cluster and the client, install
`grpcio`, `protobuf` and `streamlit` instead.

Alternatively, install the package in editable mode with optional groups:

```sh
python -m pip install -e ".[llm,app]"
```

The LLM service downloads `TinyLlama/TinyLlama-1.1B-Chat-v1.0` from Hugging
Face the first time it starts, which needs internet access unless the model is
already cached.

## Run

Run every command from the repository root. Each process runs in its own
terminal. Start the LLM service first, then the nodes, then a client.

### 1. Start the LLM service

```sh
python -m distributed_inventory.llm --port 50051
```

Options: `--model` selects a different Hugging Face text-generation model.
The service listens on port `50051` by default.

### 2. Start the inventory nodes

#### Option A: command line, one node per terminal:

```sh
python -m distributed_inventory.server --port 50052
python -m distributed_inventory.server --port 50053
python -m distributed_inventory.server --port 50054
```

Each node's peers default to the three localhost nodes. For nodes on other
machines, pass `--host` with an address peers can reach, and `--peers` with
the full list of peer `host:port` addresses:

```sh
python -m distributed_inventory.server --host 192.168.1.20 --port 50052 \
    --peers 192.168.1.21:50052 192.168.1.22:50052 --llm 192.168.1.30:50051
```

#### Option B: Streamlit app:

Allow a few seconds after startup for the cluster to elect a leader. State
files, if created, are written to the current directory as
`raft_state_<host>_<port>.json`. Use `--state-dir` to change the location.

```sh
streamlit run app.py
```

Choose a mode at the top of the page:

- **Server:** create a node by entering an IP or hostname, a port, peer nodes
  and the LLM address. The Existing nodes dropdown selects a running node to
  show its role, term, commit index, inventory, node log and leader election
  log. The logs and status refresh every second. Stop a node with the Stop
  node button.
- **Client:** edit the cluster node list (IP and port per row, add or delete
  rows), enter credentials, an item and a quantity, then run the walkthrough.
  Each step is shown as it happens.

Nodes started from the app keep running until they are stopped or the
Streamlit process exits.

### 3. Client walkthrough (command line)

For Streamlit:
Use the Client Mode in the Streamlit app. See [the above section](#option-b-streamlit-app)

For command line:
```sh
python -m distributed_inventory.client
python -m distributed_inventory.client --nodes localhost:50052 localhost:50053 localhost:50054
```

The client tries the nodes in order until one accepts the order as leader. It
then reads inventory, asks for all three AI insights, and logs out.

### 4. Stress script (optional)

```sh
python -m distributed_inventory.client.stress_test --node localhost:50052
```

This sends 15 concurrent `ItemA` orders of 10 units each directly to the given
node. It assumes that node is the leader; unlike the walkthrough client, it does
not search for or follow the leader. Run it against a fresh cluster if you want
to compare with the sample starting quantity.

## gRPC API overview

The `inventory` protobuf package declares:

| Service | Methods | Purpose |
|---|---|---|
| `ClientService` | `login`, `logout`, `post`, `get` | Client-facing operations |
| `RaftService` | `requestVote`, `appendEntries` | Election and log replication |
| `LLMService` | `getLLMAnswer` | AI-generated text insights |
| `AppServerService` | `processBusinessRequest` | Declared in the schema, but not registered by the current server |

The client-facing `post` operation currently recognizes the type `order`.
`get` recognizes `inventory_levels`, `demand_prediction`,
`reorder_suggestions`, and `inventory_analytics`.

## Current behavior and limitations

- **Authentication is a stub.** Any non-empty username and password are
  accepted, a fixed mock token is returned, and the token is not validated by
  the inventory operations. This is not suitable for real users or deployment.
- **This is a simplified Raft implementation.** It is intended as a
  learning/demo system, not a fully validated consensus implementation or a
  production cluster.
- **Inventory is in memory.** Each node starts with the sample quantities.
  The JSON Raft state file stores term, vote, and log data, but the committed
  inventory state and commit/application indexes are not durably snapshotted.
  Restarting a node therefore does not reliably restore the last inventory.
- **Concurrent order acceptance is not atomic.** The leader checks stock
  before appending a command, while committed commands are applied later. With
  concurrent requests, more than the available stock can be acknowledged;
  the state-machine loop avoids making inventory negative by skipping a
  decrement it cannot fulfill. The stress script's expected final value is
  therefore a demonstration goal, not a guaranteed result.
- **Reads are local.** Inventory reads use the contacted node's in-memory
  state; the code does not route reads to the leader or provide a read
  consistency guarantee.
- **AI inputs are limited.** Demand prediction uses fixed sample history, not
  an order-history data store. Reorder and analytics prompts use the current
  node's inventory snapshot. The model output is generated text, not a
  validated forecast or an automatic stock update.
- **Transport is unauthenticated.** The gRPC servers use insecure channels and
  are configured for local development.
- **Event logs are in memory.** The node and election logs shown in the
  Streamlit app keep the most recent 1000 events per node and are lost when
  the node stops.
- **No automated test suite.** There is no lockfile, and the repository has no
  automated tests.

## Regenerating protobuf bindings

The generated files are checked in, so regeneration is not needed to run the
project. If you edit `distributed_inventory/proto/system.proto`, install
`grpcio-tools` in the active environment, then run from the `proto` directory:

```sh
cd distributed_inventory/proto
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. ./system.proto
```

Regeneration replaces `system_pb2_grpc.py`'s absolute `import system_pb2` with
one that works in the package. Change that line back to
`from . import system_pb2 as system__pb2`. Keep the `grpcio-tools` version
compatible with the installed `grpcio` and protobuf runtime, since the
generated gRPC module enforces a minimum runtime version.
