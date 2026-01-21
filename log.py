from dataclasses import dataclass, field
from raft.messages import LogEntry


@dataclass
class RaftLog:
    """
    In Raft the indexing is 1-based.
    """

    entries: list[LogEntry] = field(default_factory=list)

    def append(self, entry: LogEntry) -> None:
        self.entries.append(entry)

    def get_entry(self, index: int) -> LogEntry | None:
        if 0 <= index-1 < len(self.entries):
            return self.entries[index-1]
        return None

    def last_index(self) -> int:
        return len(self.entries)

    def last_term(self) -> int:
        if self.entries:
            return self.entries[-1].term
        return 0

    def get_from(self, start: int, end: int) -> list[LogEntry]:
        return self.entries[start-1:end]

    def delete_from(self, index: int) -> None:
        if 0 <= index-1 < len(self.entries):
            self.entries = self.entries[:index-1]

    def match(self, index: int, term: int) -> bool:
        if index == 0:
            return True
        entry = self.get_entry(index)
        return entry is not None and entry.term == term
