from raft.state_machine.base import StateMachineBase


class KVStore(StateMachineBase):
    """A simple key-value store state machine for Raft."""

    def __init__(self) -> None:
        self.store = {}

    def apply(self, command: str) -> None:
        """Apply a command to the key-value store.

        The command is expected to be in the format "SET key value" or "DELETE key".

        Args:
            command (str): The command to apply.
        """
        parts = command.split()
        if not parts:
            return "ERROR: Empty command"

        action = parts[0].upper()
        if action == "SET":
            if len(parts) != 3:
                return "ERROR: SET requires key and value"
            self.store[parts[1]] = parts[2]
            return "OK"
        elif action == "GET":
            if len(parts) != 2:
                return "ERROR: GET requires key"
            return self.store.get(parts[1], "NOT_FOUND")
        elif action == "DELETE":
            if len(parts) != 2:
                return "ERROR: DELETE requires key"
            if parts[1] in self.store:
                del self.store[parts[1]]
            return "OK"
        else:
            return f"ERROR: unknown command {command}"
