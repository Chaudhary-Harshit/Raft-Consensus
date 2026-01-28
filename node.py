import asyncio
import random
from raft.messages import RequestVote, AppendEntries
from raft.serializer.base import SerializerBase
from raft.state import NodeState, PersistentState, VolatileState, LeaderVolatileState
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
        election_timeout_min: float = 0.150,
        election_timeout_max: float = 0.300,
        heartbeat_interval: float = 0.050,
    ):
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
        await self._transport.stop()

    async def _handle_message(self, sender: str, data: bytes) -> None:
        """Handle incoming messages from other nodes."""
        pass

    def _reset_election_timeout(self) -> None:
        """Reset/start the election timeout timer."""
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
            prev_log_index = self._leader_state.next_index[peer_id] - 1
            prev_log_term = 0
            if prev_log_index > 0:
                prev_log_term = self._persistent_state.log[prev_log_index - 1].term
            append_entries = AppendEntries(
                term=self._persistent_state.current_term,
                leader_id=self._node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=[],
                leader_commit=self._volatile_state.commit_index,
            )
            data = self._serializer.serialize(append_entries)
            await self._transport.send(peer_address, data)
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
