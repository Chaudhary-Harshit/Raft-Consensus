from raft.storage.base import StorageBase
from raft.messages import LogEntry


class MemoryStorage(StorageBase):
    def __init__(self):
        self._term = 0
        self._voted_for = None
        self._log = []

    def save_term(self, term: int) -> None:
        self._term = term

    def load_term(self) -> int:
        return self._term

    def save_voted_for(self, candidate_id: str | None) -> None:
        self._voted_for = candidate_id

    def load_voted_for(self) -> str | None:
        return self._voted_for

    def append_entries(self, entries: list[LogEntry]) -> None:
        self._log.extend(entries)

    def load_log(self) -> list[LogEntry]:
        return self._log.copy()

    def truncate_log(self, from_index: int) -> None:
        if 0 <= from_index-1 < len(self._log):
            self._log = self._log[:from_index-1]
