from abc import ABC, abstractmethod


class StateMachineBase(ABC):
    """Interface for the state machine that Raft applies committed commands to."""

    @abstractmethod
    def apply(self, command: str) -> None:
        """Apply a command to the state machine.

        Args:
            command (str): The command to apply.
        """
        pass
