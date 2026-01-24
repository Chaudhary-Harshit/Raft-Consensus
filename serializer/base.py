from abc import ABC, abstractmethod
from raft.messages import RaftMessage


class SerializerBase(ABC):
    """Abstract base class for serializers in a Raft implementation so that we can have different serialization mechanisms like JSON, Protobuf, MsgPack etc."""

    @abstractmethod
    def serialize(self, message: RaftMessage) -> bytes:
        """Serialize an object into bytes.

        Args:
            message (RaftMessage): The message to be serialized.

        Returns:
            bytes: The serialized data.
        """
        pass

    @abstractmethod
    def deserialize(self, data: bytes) -> RaftMessage:
        """Deserialize bytes back into an object.

        Args:
            data (bytes): The data to be deserialized.

        Returns:
            RaftMessage: The deserialized data.
        """
        pass
