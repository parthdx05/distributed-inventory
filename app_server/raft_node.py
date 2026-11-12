import threading
import time
import random
import json
import grpc
import sys
import os

sys.path.append('../proto')
import system_pb2
import system_pb2_grpc

class RaftNode(system_pb2_grpc.RaftServiceServicer):
    def __init__(self, node_id, peers, inventory_state):
        self.node_id = node_id
        self.peers = peers 
        self.inventory = inventory_state 
        self.lock = threading.Lock()
        
        # Persistent storage file mapping to the port
        self.state_file = f"raft_state_{node_id.replace(':', '_')}.json"
        
        # Persistent state on all servers
        self.current_term = 0
        self.voted_for = None
        self.log = [] 
        self.load_state() # Load from disk if recovering from crash
        
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
        while True:
            time.sleep(0.1)
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
        
        print(f"Node {self.node_id} starting election for term {self.current_term}")
        
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
                    print(f"Node {self.node_id} elected LEADER for term {self.current_term}")
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
                
            last_log_index = len(self.log) - 1
            last_log_term = self.log[-1]['term'] if self.log else 0
            
            log_is_ok = (request.last_log_term > last_log_term) or \
                        (request.last_log_term == last_log_term and request.last_log_index >= last_log_index)
            
            if request.term == self.current_term and (self.voted_for in (None, request.from_node)) and log_is_ok:
                self.voted_for = request.from_node
                self.last_heartbeat = time.time()
                self.save_state()
                return system_pb2.VoteReply(from_node=self.node_id, to_node=request.from_node, term=self.current_term, vote_granted=True)
                
            return system_pb2.VoteReply(from_node=self.node_id, to_node=request.from_node, term=self.current_term, vote_granted=False)

    def heartbeat_sender(self):
        while True:
            time.sleep(0.5)
            with self.lock:
                if self.state == "LEADER":
                    for peer in self.peers:
                        threading.Thread(target=self.send_append_entries, args=(peer,), daemon=True).start()

    def send_append_entries(self, peer):
        with self.lock:
            if self.state != "LEADER": return
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
            
            if request.prev_index >= 0:
                if request.prev_index >= len(self.log) or self.log[request.prev_index]['term'] != request.prev_term:
                    return system_pb2.AppendEntriesReply(from_node=self.node_id, to_node=request.from_node, term=self.current_term, entry_appended=False, match_index=-1)
            
            new_entries = [json.loads(e) for e in request.entries]
            if new_entries:
                self.log = self.log[:request.prev_index + 1] + new_entries
                self.save_state()
                
            if request.commit_index > self.commit_index:
                self.commit_index = min(request.commit_index, len(self.log) - 1)
                
            return system_pb2.AppendEntriesReply(from_node=self.node_id, to_node=request.from_node, term=self.current_term, entry_appended=True, match_index=len(self.log)-1)

    def replicate_command(self, command):
        with self.lock:
            if self.state != "LEADER":
                return False
            self.log.append({'term': self.current_term, 'command': command})
            self.save_state()
            return True

    def apply_commits(self):
        while True:
            time.sleep(0.1)
            with self.lock:
                while self.commit_index > self.last_applied:
                    self.last_applied += 1
                    command = self.log[self.last_applied]['command']
                    
                    if command['action'] == 'DECREMENT':
                        item = command['item']
                        qty = command['qty']
                        if self.inventory.get(item, 0) >= qty:
                            self.inventory[item] -= qty
                            print(f"[State Machine] Applied: Decremented {qty} of {item}. New stock: {self.inventory[item]}")