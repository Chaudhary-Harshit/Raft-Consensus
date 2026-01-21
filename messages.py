from dataclasses import dataclass


@dataclass(frozen=True)
class LogEntry:
    term: int
    command: str  # command for state machine


@dataclass
class RequestVote:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteResponse:  # voter's reply
    term: int
    vote_granted: bool


@dataclass
class AppendEntries:
    term: int
    leader_id: str
    prev_log_index: int  # index of log entry immediately preceding new ones
    prev_log_term: int  # term of prev_log_index entry
    entries: list[LogEntry]  # log entries to store (empty for heartbeat)
    leader_commit: int  # leader's commit index


@dataclass
class AppendEntriesResponse:  # follower's reply
    term: int
    success: bool
