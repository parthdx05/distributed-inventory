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
LLM service that produces inventory-related text insights.

The project is intended to demonstrate service boundaries, gRPC communication,
leader-based writes, log replication, and an AI-backed read path. It is not a
production inventory or consensus system; see [Current behavior and
limitations](#current-behavior-and-limitations).

## What it does

- Accepts client login, logout, inventory-read, and order requests over gRPC.
- Runs three application/Raft nodes on ports `50052`, `50053`, and `50054`.
- Elects a leader and replicates order commands to peer nodes.
- Applies committed decrement commands to each node's in-memory inventory.
- Sends demand-prediction, reorder-suggestion, and inventory-analytics prompts
  to a separate gRPC service running TinyLlama locally.
- Includes a basic client walkthrough and a concurrent-order stress script.

Each node starts with the same sample inventory:

| Item | Starting quantity |
|---|---:|
| `ItemA` | 100 |
| `ItemB` | 50 |

## Architecture

```text
 client/client.py
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
 |   Each node exchanges Raft RPCs with both peers.    |
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

Each application process hosts both the client-facing `ClientService` and the
inter-node `RaftService`. The LLM process is separate. The node addresses and
peer membership are currently hard-coded in the source.

## Request flow

### Placing an order

1. The client logs in through node `50052`.
2. It sends an `order` request containing a JSON item name and quantity.
3. If the contacted node is not the leader, it returns
   `REDIRECT_TO_LEADER`. The client tries the next configured node.
4. The leader checks its local stock, appends a `DECREMENT` command to its
   Raft log, and sends log entries to peers using `appendEntries`.
5. Once the leader advances the command's commit index, the request returns
   `ORDER_SUCCESS`. A background state-machine thread applies committed
   decrements to inventory.
6. The client reads inventory from the selected node and then logs out.

The order payload is JSON shaped like `{"item":"ItemA","qty":10}`. The public
gRPC messages and methods are defined in [`proto/system.proto`](proto/system.proto).

### Reading inventory and requesting insights

- `inventory_levels` returns the node's current in-memory item quantities.
- `demand_prediction` sends sample historical data hard-coded in the
  application server to the LLM service.
- `reorder_suggestions` sends the current inventory JSON to the LLM service.
- `inventory_analytics` sends the current inventory JSON to the LLM service.

The LLM service builds a task-specific prompt, generates up to 150 new tokens,
and returns the generated text. The model is loaded with `device=-1`, so this
implementation runs inference on CPU.

## Repository layout

```text
app_server/
  raft_node.py       Raft RPC handlers, election/heartbeat threads, log and state machine
  server.py          gRPC application server and client-facing inventory operations
client/
  client.py          Login, order, inventory read, AI insight, and logout walkthrough
  stress_test.py     Concurrent order requests against node 50052
llm_server/
  llm_service.py     TinyLlama-backed gRPC service
proto/
  system.proto       Source gRPC/protobuf service and message definitions
  system_pb2.py      Generated protobuf messages
  system_pb2_grpc.py Generated gRPC client/server bindings
```

## Requirements

- Python with a gRPC runtime compatible with the checked-in generated bindings.
  The workspace used to inspect this project has Python 3.14, `grpcio` 1.84.0,
  and `protobuf` 7.36.1. The generated gRPC code requires `grpcio >= 1.84.0`;
  the generated protobuf module requires protobuf 7.35.1 or newer.
- `transformers` and `torch` to run the LLM service.
- Internet access the first time the LLM service runs, unless the model is
  already cached locally.

There is no checked-in dependency manifest. From the repository root, create an
environment and install the dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install "grpcio>=1.84.0" "protobuf>=7.35.1"
python -m pip install transformers torch
```

```sh
python -m venv .venv
.\.venv\Scripts\activate
python -m pip install --upgrade pip
python -m pip install "grpcio>=1.84.0" "protobuf>=7.35.1"
python -m pip install transformers torch
```

The LLM package install may be large. `grpcio-tools` is only needed if you
change the protobuf definition and want to regenerate the checked-in bindings.

## Run locally
Start each process in a separate terminal opened at the repository root. The
scripts append `../proto` to Python's import path relative to the **current
working directory**, so change to each script's directory before running it.

### 1. Start the LLM service
#### Windows / PowerShell
```powershell
Set-Location .\llm_server
..\.venv\Scripts\python.exe .\llm_service.py
```

#### Linux / MacOS / bash
```sh
cd .\llm_server
..\.venv\bin\python .\llm_service.py
```
---

It listens on `localhost:50051` and loads
`TinyLlama/TinyLlama-1.1B-Chat-v1.0` at startup. The first launch may take time
to download and initialize the model.

### 2. Start the three application/Raft nodes
Run each command in its own terminal:

#### Windows / PowerShell
```powershell
Set-Location .\app_server
..\.venv\Scripts\python.exe .\server.py 50052
```

```powershell
Set-Location .\app_server
..\.venv\Scripts\python.exe .\server.py 50053
```

```powershell
Set-Location .\app_server
..\.venv\Scripts\python.exe .\server.py 50054
```

#### Linux / MacOS / bash
```sh
cd ./app_server
../.venv/bin/python ./server.py 50052
```

```sh
cd ./app_server
../.venv/bin/python ./server.py 50053
```

```sh
cd ./app_server
../.venv/bin/python ./server.py 50054
```
---

The nodes use randomized election timeouts, so allow a few seconds for the
cluster to elect a leader before sending orders. The state files, if created,
are written to the current directory as `raft_state_localhost_50052.json` and
corresponding files for the other ports.

### 3. Run the client walkthrough

#### Windows / Powershell
```powershell
Set-Location .\client
..\.venv\Scripts\python.exe .\client.py
```

#### Linux / MacOS / bash
```sh
cd ./client
../.venv/bin/python ./client.py
```
---

The client tries the configured nodes for an order until one does not respond
with `REDIRECT_TO_LEADER`. It then reads inventory, asks for all three AI
insights, and logs out.

### 4. Run the stress script (optional)

#### Windows / Powershell
```powershell
Set-Location .\client
..\.venv\Scripts\python.exe .\stress_test.py
```

#### Linux / MacOS / bash
```sh
cd ./client
../.venv/bin/python ./stress_test.py
```
---

This script sends 15 concurrent `ItemA` orders of 10 units each directly to
`localhost:50052`. It assumes that port is currently the leader; unlike the
walkthrough client, it does not search for or follow the leader. Run it against
a fresh cluster if you want to compare with the sample starting quantity.

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
- **This is a simplified Raft implementation.** It uses three hard-coded
  localhost nodes and is intended as a learning/demo system, not a fully
  validated consensus implementation or a production cluster.
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
- **Transport is unauthenticated.** The gRPC servers use insecure local
  channels and are configured for local development.
- **No automated test suite or dependency lockfile is included** in the
  repository.

## Regenerating protobuf bindings

The generated files are checked in, so regeneration is not needed to run the
project. If you edit `system.proto`, install `grpcio-tools` in the active
environment, then run this from the `proto` directory:

#### Windows / Powershell
```powershell
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. .\system.proto
```

#### Linux / MacOS / bash
```sh
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. .\system.proto
```

Keep the `grpcio-tools` version compatible with the installed `grpcio` and
protobuf runtime, since the generated gRPC module enforces a minimum runtime
version.
