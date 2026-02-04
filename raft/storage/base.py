from abc import ABC, abstractmethod
from raft.messages import LogEntry


class StorageBase(ABC):
    """Interface Class so that we can have any Memory storage(RAM) or File Storage(disk) implementation for a Raft Node (we can choose any implementation while creating a Raft Node as per the requirement)"""

    @abstractmethod
    def save_term(self, term: int) -> None:
        """Save the current term to storage"""
        pass

    @abstractmethod
    def load_term(self) -> int:
        """Load the current term from storage"""
        pass

    @abstractmethod
    def save_voted_for(self, candidate_id: str | None) -> None:
        """Save the candidate id voted for in current term to storage"""
        pass

    @abstractmethod
    def load_voted_for(self) -> str | None:
        """Load the candidate id voted for in current term from storage"""
        pass

    @abstractmethod
    def append_entries(self, entries: list[LogEntry]) -> None:
        """Append log entries to storage"""
        pass

    @abstractmethod
    def load_log(self) -> list[LogEntry]:
        """Load log entries from storage"""
        pass

    @abstractmethod
    def truncate_log(self, from_index: int) -> None:
        """Truncate log entries from storage starting from index"""
        pass
