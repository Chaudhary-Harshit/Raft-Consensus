import asyncio
import random
from raft.messages import RequestVote, AppendEntries, RequestVoteResponse, AppendEntriesResponse
from raft.serializer.base import SerializerBase
from raft.state import NodeState, PersistentState, VolatileState, LeaderVolatileState
from raft.state_machine.base import StateMachineBase
from raft.storage.base import StorageBase
from raft.transport.base import TransportBase


class RaftNode:
    """Implementation of Raft consensus node."""

    def __init__(
        self,
        node_id: str,
        peers: dict[str, str],
        transport: TransportBase,
        storage: StorageBase,
        serializer: SerializerBase,
        state_machine: StateMachineBase,
        election_timeout_min: float = 0.150,
        election_timeout_max: float = 0.300,
        heartbeat_interval: float = 0.050,
    ):
        # Initialize State Machine
        self._state_machine = state_machine
        self._apply_task = None

        # Node Identity
        self._node_id = node_id
        self._peers = peers  # mapping of node_id to address

        # Components/Interfaces
        self._transport = transport
        self._storage = storage
        self._serializer = serializer

        # State of Node (start as follower)
        self._node_state = NodeState.FOLLOWER

        # Persistent state (survives crashes)
        self._persistent_state = PersistentState()

        # Volatile state (rebuilds after crash)
        self._volatile_state = VolatileState()

        # Leader volatile state (only for leaders)
        self._leader_state: LeaderVolatileState | None = None  # Created when node becomes leader

        self._leader_id = None  # Current leader's node_id
        self._election_timeout_task = None
        self._heartbeat_interval_task = None

        # Timeout Configurations
        self._election_timeout_min = election_timeout_min  # Minimum wait before starting election (default: 150ms)
        self._election_timeout_max = election_timeout_max  # Maximum wait before starting election (has to be randomized to avoid split vote (all nodes start election at once and the no winner), default: 300ms)
        self._heartbeat_interval = heartbeat_interval  # How often leader sends heartbeats (default: 50ms)

        # Vote Tracking in Elections
        self._votes_received = set()  # Track votes received in current election

    async def start(self, host: str, port: int) -> None:
        """Start the Raft node and begin participating in the cluster."""

        self._persistent_state.current_term = self._storage.load_term()
        self._persistent_state.voted_for = self._storage.load_voted_for()
        self._persistent_state.log = self._storage.load_log()

        self._apply_task = asyncio.create_task(self._apply_loop())

        self._transport.register_handler(self._handle_message)

        await self._transport.start(host, port)
        self._reset_election_timeout()

    async def stop(self) -> None:
        if self._election_timeout_task:
            self._election_timeout_task.cancel()
            self._election_timeout_task = None
        if self._heartbeat_interval_task:
            self._heartbeat_interval_task.cancel()
            self._heartbeat_interval_task = None
        if self._apply_task:
            self._apply_task.cancel()
            self._apply_task = None
        await self._transport.stop()

    async def _handle_message(self, sender: str, data: bytes) -> None:
        """Handle incoming messages from other nodes."""

        message_object = self._serializer.deserialize(data)

        # Step down if message term is higher than current term meaning that there is a more up-to-date leader/candidate
        if message_object.term > self._persistent_state.current_term:
            self._step_down(message_object.term)

        # Dispatch as per the type of message
        response = None

        if isinstance(message_object, RequestVote):
            response = self._handle_request_vote(message_object)
        elif isinstance(message_object, RequestVoteResponse):
            await self._handle_request_vote_response(message_object)
        elif isinstance(message_object, AppendEntries):
            response = self._handle_append_entries(message_object)
        elif isinstance(message_object, AppendEntriesResponse):
            self._handle_append_entries_response(message_object)

        # Send response back to the sender if handler returned one
        if response is not None:
            if isinstance(message_object, RequestVote):
                peer_address = self._peers[message_object.candidate_id]
            elif isinstance(message_object, AppendEntries):
                peer_address = self._peers[message_object.leader_id]

            data = self._serializer.serialize(response)
            await self._transport.send(peer_address, data)

    def _handle_request_vote(self, message: RequestVote) -> RequestVoteResponse:
        """Handle incoming RequestVote RPCs from candidates."""

        if message.term < self._persistent_state.current_term:
            return RequestVoteResponse(term=self._persistent_state.current_term, vote_granted=False, sender_id=self._node_id)

        # To grant a vote we need two conditions to be True

        # CONDITION 1: Have not voted for anybody in this term or have already voted for the candidate
        can_vote = (
            self._persistent_state.voted_for is None or
            self._persistent_state.voted_for == message.candidate_id
        )

        # CONDITION 2: Candidate's log is at least as up-to-date as receiver's log
        receiver_last_log_term = self._persistent_state.log[-1].term if self._persistent_state.log else 0
        receiver_last_log_index = len(self._persistent_state.log)

        # If the candidate's log term is greater than receiver's last log term, or if they are equal and candidate's last log index is greater than or equal to receiver's last log index means candidate is more up to date
        candidate_up_to_date = (
            message.last_log_term > receiver_last_log_term or
            (message.last_log_term == receiver_last_log_term and message.last_log_index >= receiver_last_log_index)
        )

        vote_granted = can_vote and candidate_up_to_date
        current_term = self._persistent_state.current_term
        if vote_granted:
            self._persistent_state.voted_for = message.candidate_id
            self._storage.save_voted_for(message.candidate_id)
            self._reset_election_timeout()  # Reset election timeout since we granted a vote
            return RequestVoteResponse(term=current_term, vote_granted=True, sender_id=self._node_id)
        else:
            return RequestVoteResponse(term=current_term, vote_granted=False, sender_id=self._node_id)

    async def _handle_request_vote_response(self, message: RequestVoteResponse) -> None:
        """Handle incoming RequestVoteResponse RPCs from voters."""

        # Ignore if no longer a candidate
        if self._node_state != NodeState.CANDIDATE:
            return

        # Ignore stale response (from old election)
        if message.term < self._persistent_state.current_term:
            return

        if message.vote_granted:
            self._votes_received.add(message.sender_id)
            if self._has_majority():
                await self._become_leader()

    def _handle_append_entries(self, message: AppendEntries) -> AppendEntriesResponse:
        """Handle incoming AppendEntries RPCs from the leader. (heartbeat or log replication)"""

        if message.term < self._persistent_state.current_term:
            return AppendEntriesResponse(term=self._persistent_state.current_term, success=False, sender_id=self._node_id)

        self._leader_id = message.leader_id
        self._reset_election_timeout()  # Reset election timeout since we received a heartbeat and leader is alive

        if self._node_state != NodeState.FOLLOWER:
            self._node_state = NodeState.FOLLOWER

        # Log Consistency Check
        if message.prev_log_index > 0:
            # If we do not have an entry at or beyond prev_log_index or the term does not match, we reject the AppendEntries
            if message.prev_log_index > len(self._persistent_state.log):
                return AppendEntriesResponse(term=self._persistent_state.current_term, success=False, sender_id=self._node_id)
            if message.prev_log_term != self._persistent_state.log[message.prev_log_index - 1].term:
                return AppendEntriesResponse(term=self._persistent_state.current_term, success=False, sender_id=self._node_id)

        # Processing Entries if it is not just a heartbeat (append_entries heartbeat would have empty entries)

        if message.entries:
            for i, entry in enumerate(message.entries):
                log_index = message.prev_log_index + 1 + i  # 1-based Raft index for this entry
                if log_index <= len(self._persistent_state.log):
                    # Entry exists at this index — check for conflict
                    if self._persistent_state.log[log_index - 1].term != entry.term:
                        # Conflict: delete this entry and everything after it, then append remaining new entries
                        self._persistent_state.log = self._persistent_state.log[:log_index - 1]
                        self._storage.truncate_log(log_index)
                        self._persistent_state.log.extend(message.entries[i:])
                        self._storage.append_entries(message.entries[i:])
                        break
                else:
                    # No existing entry at this index — append all remaining new entries
                    self._persistent_state.log.extend(message.entries[i:])
                    self._storage.append_entries(message.entries[i:])
                    break

        # Update commit index
        if message.leader_commit > self._volatile_state.commit_index:
            self._volatile_state.commit_index = min(message.leader_commit, len(self._persistent_state.log))

        return AppendEntriesResponse(term=self._persistent_state.current_term, success=True, sender_id=self._node_id)

    def _handle_append_entries_response(self, message: AppendEntriesResponse) -> None:
        """Handle incoming AppendEntriesResponse RPCs from followers."""

        # Only leaders handle AppendEntries responses
        if self._node_state != NodeState.LEADER or not self._leader_state:
            return

        # Ignore stale response
        if message.term < self._persistent_state.current_term:
            return

        if message.success:
            # Follower's log matched — update match_index and next_index
            self._leader_state.match_index[message.sender_id] = self._leader_state.next_index[message.sender_id] - 1
            self._leader_state.next_index[message.sender_id] = self._leader_state.match_index[message.sender_id] + 1
        else:
            # Follower's log did not match — decrement next_index and retry with earlier entry
            self._leader_state.next_index[message.sender_id] = max(1, self._leader_state.next_index[message.sender_id] - 1)
            return

        # Try to advance Commit index
        for index in range(self._volatile_state.commit_index + 1, len(self._persistent_state.log) + 1):
            # Count how many nodes have this entry (leader always has it)
            replicated_count = 1  # counting self
            for peer_id in self._peers:
                if self._leader_state.match_index.get(peer_id, 0) >= index:
                    replicated_count += 1

            # Only commit if a majority have replicated and the log entry is from current term
            if replicated_count > (len(self._peers) + 1) // 2 and self._persistent_state.log[index - 1].term == self._persistent_state.current_term:
                self._volatile_state.commit_index = index
            else:
                break

    def _step_down(self, term: int) -> None:
        """Step Down to follower state when the node receives a message with higher term."""

        self._persistent_state.current_term = term
        self._storage.save_term(self._persistent_state.current_term)

        self._persistent_state.voted_for = None
        self._storage.save_voted_for(None)

        self._votes_received.clear()

        self._node_state = NodeState.FOLLOWER
        self._leader_id = None

        if self._heartbeat_interval_task:
            self._heartbeat_interval_task.cancel()
            self._heartbeat_interval_task = None

        self._leader_state = None
        self._reset_election_timeout()

    def _reset_election_timeout(self) -> None:
        """
        Reset/start the election timeout timer for this node.
        If a node granted a vote, a candidate must be actively trying to become leader. By resetting the timeout, we are giving that candidate time to win the election and start sending heartbeats — rather than immediately timing out ourself and starting a competing election.

        Without this reset, consider a 5-node cluster:
        1. Node A starts election, requests votes
        2. Nodes B, C, D grant votes
        3. But B, C, D are all close to their own election timeouts
        4. Before A can win and send heartbeats, B or C times out and starts a new election with a higher term
        5. This disrupts A's leadership, potentially causing repeated failed elections

        Resetting the timeout reduces unnecessary election churn.
        """
        if self._election_timeout_task:
            self._election_timeout_task.cancel()

        timeout = random.uniform(self._election_timeout_min, self._election_timeout_max)

        self._election_timeout_task = asyncio.create_task(self._election_timeout_handler(timeout))

    async def _election_timeout_handler(self, timeout: float) -> None:
        """Wait for timeout and then start the election, if not already a leader."""
        await asyncio.sleep(timeout)
        if self._node_state != NodeState.LEADER:
            await self._start_election()

    async def _start_election(self) -> None:
        """Start a new election (become candidate, request votes)."""

        # Become candidate
        self._node_state = NodeState.CANDIDATE

        # Increment the current term and save it to persistent storage
        self._persistent_state.current_term += 1
        self._storage.save_term(self._persistent_state.current_term)

        # Vote for self and save to persistent storage
        self._persistent_state.voted_for = self._node_id
        self._storage.save_voted_for(self._node_id)

        # Reset votes received
        self._votes_received.clear()
        self._votes_received.add(self._node_id)  # Vote for self

        self._reset_election_timeout()  # If election fails (split vote), we need to timeout and try again.

        if self._has_majority():  # single node cluster
            await self._become_leader()
            return

        # send to all peers in parallel, this sends to all peers concurrently
        await asyncio.gather(*[
            self._send_request_vote(peer_id, peer_address)
            for peer_id, peer_address in self._peers.items()
        ], return_exceptions=True)

    def _has_majority(self) -> bool:
        """Check if the node has received majority votes."""
        total_nodes = len(self._peers) + 1  # Including self
        return len(self._votes_received) > total_nodes // 2

    async def _become_leader(self) -> None:

        # Change the state to leader
        self._node_state = NodeState.LEADER
        self._leader_id = self._node_id

        # Cancel election timeout task as we are now the leader
        if self._election_timeout_task:
            self._election_timeout_task.cancel()
            self._election_timeout_task = None

        # Initialize leader volatile state
        last_log_index = len(self._persistent_state.log)
        self._leader_state = LeaderVolatileState(
            next_index={peer_id: last_log_index + 1 for peer_id in self._peers},
            match_index={peer_id: 0 for peer_id in self._peers},
        )

        # Start Heatbeat Loop
        self._heartbeat_interval_task = asyncio.create_task(self._heartbeat_loop())

        # Send initial heartbeats to all followers
        await self._send_heartbeats()

    async def _heartbeat_loop(self) -> None:
        """Periodically send heartbeats to followers."""
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            try:
                await self._send_heartbeats()
            except Exception:
                pass

    async def _send_heartbeats(self) -> None:
        """Send AppendEntries (heartbeats) to all the followers."""

        async def _send_to_peer(peer_id, peer_address):
            next_index = self._leader_state.next_index[peer_id]
            prev_log_index = next_index - 1
            prev_log_term = 0
            if prev_log_index > 0:
                prev_log_term = self._persistent_state.log[prev_log_index - 1].term

            entries = self._persistent_state.log[next_index - 1:]  # Entries to send (could be empty for heartbeat)

            append_entries = AppendEntries(
                term=self._persistent_state.current_term,
                leader_id=self._node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=self._volatile_state.commit_index,
            )
            data = self._serializer.serialize(append_entries)
            await self._transport.send(peer_address, data)
            if entries:
                self._leader_state.next_index[peer_id] = next_index + len(entries)
        await asyncio.gather(*[
            _send_to_peer(peer_id, peer_address)
            for peer_id, peer_address in self._peers.items()
        ], return_exceptions=True)

    async def _send_request_vote(self, peer_id: str, peer_address: str) -> None:
        """Send RequestVote RPC to a peer."""

        last_log_index = len(self._persistent_state.log)
        last_log_term = 0
        if self._persistent_state.log:
            last_log_term = self._persistent_state.log[-1].term
        request = RequestVote(
            term=self._persistent_state.current_term,
            candidate_id=self._node_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term,
        )
        data = self._serializer.serialize(request)
        await self._transport.send(peer_address, data)

    # A background async task that watches for committed but unapplied entries
    async def _apply_loop(self) -> None:
        """Apply committed log entries to the state machine."""
        while True:
            if self._volatile_state.last_applied < self._volatile_state.commit_index:
                self._volatile_state.last_applied += 1
                entry = self._persistent_state.log[self._volatile_state.last_applied - 1]
                # Apply the command to the state machine
                self._state_machine.apply(entry.command)
            else:
                await asyncio.sleep(0.01)  # Sleep briefly to avoid busy waiting (pause 10ms, let other tasks runa)
