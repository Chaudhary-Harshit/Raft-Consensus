from dataclasses import dataclass, field
from enum import Enum

from raft.messages import LogEntry


class NodeState(Enum):
    FOLLOWER = 'follower'  # default state, recieves heartbeats from the leader
    CANDIDATE = 'candidate'  # trying to become the leader
    LEADER = 'leader'  # accepts client requests and replicates log entries


@dataclass
class PersistentState:
    current_term: int = 0  # latest term node has seen
    voted_for: str | None = None  # node id voted in this term
    log: list[LogEntry] = field(default_factory=list)  # list of log entries


@dataclass
class VolatileState:  # has to be rebuilt after node crash
    commit_index: int = 0  # highest log index known to be committed
    last_applied: int = 0  # highest log index applied to the machine


@dataclass
class LeaderVolatileState:
    next_index: dict[str, int] = field(default_factory=dict)  # for each follower, index of the next log entry to send to that follower
    match_index: dict[str, int] = field(default_factory=dict)  # for each follower, index of highest log entry that leader knows has been replicated on that follower
