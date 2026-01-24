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
