from abc import ABC, abstractmethod
from typing import Awaitable, Callable


class TransportBase(ABC):
    """Abstract base class for transport mechanisms in a Raft implementation so that we can have different transport mechanisms like  TCP, UDP, gRPC etc."""

    @abstractmethod
    async def start(self, host: str, port: int) -> None:
        """Start the transport mechanism.

        Args:
            host (str): The host address to bind to.
            port (int): The port number to bind to.
        """
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Stop the transport mechanism. Close all the connections and release the ports"""
        pass

    @abstractmethod
    async def send(self, to: str, message: bytes) -> None:
        """Send a message to the specified address.

        Args:
            to (str): The address to send the message to.
            message (bytes): The message to be sent.
        """
        pass

    @abstractmethod
    def register_handler(self, handler: Callable[[str, bytes], Awaitable[None]]) -> None:
        """Register a handler (function) that would know how to process incoming messages.

        Args:
            handler (callable[[str, bytes], Awaitable[None]]): A function that takes the sender's address and the message as arguments and is a async function that returns None.
        """
        pass
