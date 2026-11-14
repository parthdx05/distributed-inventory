import collections
import json
import os
import random
import threading
import time

import grpc

from ..proto import system_pb2, system_pb2_grpc

MAX_EVENTS = 1000


class RaftNode(system_pb2_grpc.RaftServiceServicer):
    def __init__(self, node_id, peers, inventory_state, state_dir=None):
        self.node_id = node_id
        self.peers = peers
        self.inventory = inventory_state
        self.lock = threading.Lock()
        self._stop_event = threading.Event()

        # Event log for the UI: (timestamp, category, message). Categories: "election", "node".
        self._events = collections.deque(maxlen=MAX_EVENTS)
        self._events_lock = threading.Lock()

        # Persistent storage file mapping to the address
        state_name = f"raft_state_{node_id.replace(':', '_')}.json"
        self.state_file = os.path.join(state_dir or "", state_name)

        # Persistent state on all servers
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.load_state()  # Load from disk if recovering from crash

        # Volatile state on all servers
        self.commit_index = -1
        self.last_applied = -1
        self.state = "FOLLOWER"

        # Volatile state on leaders
        self.next_index = {peer: 0 for peer in peers}
        self.match_index = {peer: -1 for peer in peers}

        # Timers
        self.election_timeout = random.uniform(1.5, 3.0)
        self.last_heartbeat = time.time()

        # Start background threads
        threading.Thread(target=self.election_timer, daemon=True).start()
        threading.Thread(target=self.heartbeat_sender, daemon=True).start()
        threading.Thread(target=self.apply_commits, daemon=True).start()
        self.emit("node", f"started as FOLLOWER in term {self.current_term}")

    def stop(self):
        """Ask the background threads to exit. Persisted state is kept on disk."""
        self._stop_event.set()
        self.emit("node", "stopped")

    def emit(self, category, message):
        """Record an event for the UI and print it."""
        stamp = time.strftime("%H:%M:%S")
        with self._events_lock:
            self._events.append((stamp, category, message))
        print(f"[{self.node_id}] {message}")

    def events(self, category=None):
        """Snapshot of recorded events, oldest first, optionally filtered by category."""
        with self._events_lock:
            snapshot = list(self._events)
        if category is None:
            return snapshot
        return [e for e in snapshot if e[1] == category]

    def save_state(self):
        """Persist term and log to disk to survive node kills."""
        with open(self.state_file, 'w') as f:
            json.dump({
                'current_term': self.current_term,
                'voted_for': self.voted_for,
                'log': self.log
            }, f)

    def load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                    self.current_term = data.get('current_term', 0)
                    self.voted_for = data.get('voted_for', None)
                    self.log = data.get('log', [])
            except Exception:
                pass

    def election_timer(self):
        while not self._stop_event.wait(0.1):
            with self.lock:
                if self.state != "LEADER" and (time.time() - self.last_heartbeat) > self.election_timeout:
                    self.start_election()

    def start_election(self):
        self.state = "CANDIDATE"
        self.current_term += 1
        self.voted_for = self.node_id
        self.save_state()
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(1.5, 3.0)

        self.emit("election", f"starting election for term {self.current_term}")

        last_log_index = len(self.log) - 1
        last_log_term = self.log[-1]['term'] if self.log else 0

        for peer in self.peers:
            threading.Thread(target=self.send_request_vote, args=(peer, last_log_index, last_log_term), daemon=True).start()

    def send_request_vote(self, peer, last_log_index, last_log_term):
        try:
            channel = grpc.insecure_channel(peer)
            stub = system_pb2_grpc.RaftServiceStub(channel)
            req = system_pb2.VoteRequest(
                from_node=self.node_id, to_node=peer, term=self.current_term,
                last_log_index=last_log_index, last_log_term=last_log_term
            )
            res = stub.requestVote(req, timeout=0.5)

            with self.lock:
                if self.state == "CANDIDATE" and res.vote_granted:
                    self.emit("election", f"elected LEADER for term {self.current_term}")
                    self.state = "LEADER"
                    for p in self.peers:
                        self.next_index[p] = len(self.log)
                        self.match_index[p] = -1
        except grpc.RpcError:
            pass

    def requestVote(self, request, context):
        with self.lock:
            if request.term > self.current_term:
                self.current_term = request.term
                self.state = "FOLLOWER"
                self.voted_for = None
                self.save_state()
                self.emit("election", f"saw term {request.term} from {request.from_node}, stepping down to FOLLOWER")

            last_log_index = len(self.log) - 1
            last_log_term = self.log[-1]['term'] if self.log else 0

            log_is_ok = (request.last_log_term > last_log_term) or \
                        (request.last_log_term == last_log_term and request.last_log_index >= last_log_index)

            if request.term == self.current_term and (self.voted_for in (None, request.from_node)) and log_is_ok:
                self.voted_for = request.from_node
                self.last_heartbeat = time.time()
                self.save_state()
                self.emit("election", f"granted vote to {request.from_node} for term {self.current_term}")
                return system_pb2.VoteReply(from_node=self.node_id, to_node=request.from_node, term=self.current_term, vote_granted=True)

            self.emit("election", f"denied vote to {request.from_node} for term {request.term}")
            return system_pb2.VoteReply(from_node=self.node_id, to_node=request.from_node, term=self.current_term, vote_granted=False)

    def heartbeat_sender(self):
        while not self._stop_event.wait(0.5):
            with self.lock:
                if self.state == "LEADER":
                    for peer in self.peers:
                        threading.Thread(target=self.send_append_entries, args=(peer,), daemon=True).start()

    def send_append_entries(self, peer):
        with self.lock:
            if self.state != "LEADER":
                return
            prev_index = self.next_index[peer] - 1
            prev_term = self.log[prev_index]['term'] if prev_index >= 0 else 0
            entries = [json.dumps(entry) for entry in self.log[self.next_index[peer]:]]
            commit_index = self.commit_index
            term = self.current_term

        try:
            channel = grpc.insecure_channel(peer)
            stub = system_pb2_grpc.RaftServiceStub(channel)
            req = system_pb2.AppendEntriesRequest(
                from_node=self.node_id, to_node=peer, term=term,
                prev_index=prev_index, prev_term=prev_term,
                commit_index=commit_index, entries=entries
            )
            res = stub.appendEntries(req, timeout=0.5)

            with self.lock:
                if res.term > self.current_term:
                    self.current_term = res.term
                    self.state = "FOLLOWER"
                    self.voted_for = None
                    self.save_state()
                    self.emit("election", f"{peer} reported term {res.term}, stepping down to FOLLOWER")
                elif res.entry_appended:
                    self.next_index[peer] = prev_index + len(entries) + 1
                    self.match_index[peer] = self.next_index[peer] - 1

                    # Resolved TypeError: cast dict_values to list before appending
                    match_indices = sorted(list(self.match_index.values()) + [len(self.log) - 1])
                    majority_index = match_indices[len(match_indices) // 2]
                    if majority_index > self.commit_index and self.log[majority_index]['term'] == self.current_term:
                        self.commit_index = majority_index
                else:
                    self.next_index[peer] = max(0, self.next_index[peer] - 1)
        except grpc.RpcError:
            pass

    def appendEntries(self, request, context):
        with self.lock:
            if request.term < self.current_term:
                return system_pb2.AppendEntriesReply(from_node=self.node_id, to_node=request.from_node, term=self.current_term, entry_appended=False, match_index=-1)

            self.last_heartbeat = time.time()
            if request.term > self.current_term:
                self.current_term = request.term
                self.state = "FOLLOWER"
                self.voted_for = None
                self.save_state()
                self.emit("election", f"term {request.term} leader is {request.from_node}, following")

            if request.prev_index >= 0:
                if request.prev_index >= len(self.log) or self.log[request.prev_index]['term'] != request.prev_term:
                    return system_pb2.AppendEntriesReply(from_node=self.node_id, to_node=request.from_node, term=self.current_term, entry_appended=False, match_index=-1)

            new_entries = [json.loads(e) for e in request.entries]
            if new_entries:
                self.log = self.log[:request.prev_index + 1] + new_entries
                self.save_state()

            if request.commit_index > self.commit_index:
                self.commit_index = min(request.commit_index, len(self.log) - 1)

            return system_pb2.AppendEntriesReply(from_node=self.node_id, to_node=request.from_node, term=self.current_term, entry_appended=True, match_index=len(self.log) - 1)

    def replicate_command(self, command):
        with self.lock:
            if self.state != "LEADER":
                return False
            self.log.append({'term': self.current_term, 'command': command})
            self.save_state()
            return True

    def apply_commits(self):
        while not self._stop_event.wait(0.1):
            with self.lock:
                while self.commit_index > self.last_applied:
                    self.last_applied += 1
                    command = self.log[self.last_applied]['command']

                    if command['action'] == 'DECREMENT':
                        item = command['item']
                        qty = command['qty']
                        if self.inventory.get(item, 0) >= qty:
                            self.inventory[item] -= qty
                            self.emit("node", f"applied: decremented {qty} of {item}, new stock {self.inventory[item]}")
                        else:
                            self.emit("node", f"skipped: {item} stock too low for {qty}")
