import dataclasses
import json
from raft.messages import LogEntry, RaftMessage, MESSAGE_TYPES
from raft.serializer.base import SerializerBase


class JSONSerializer(SerializerBase):
    """JSON serializer implementation for Raft messages."""

    def serialize(self, message: RaftMessage) -> bytes:

        message_class_name = type(message).__name__
        payload = dataclasses.asdict(message)
        data_to_be_serialized = {
            'type': message_class_name,
            'payload': payload
        }
        json_string = json.dumps(data_to_be_serialized)
        return json_string.encode('utf-8')

    def deserialize(self, data: bytes) -> RaftMessage:
        json_string = data.decode('utf-8')
        data_dict = json.loads(json_string)
        message_type = data_dict['type']
        payload = data_dict['payload']
        message_class = MESSAGE_TYPES.get(message_type)
        if message_class is None:
            raise ValueError(f"Unknown message type: {message_type}")
        if message_type == 'AppendEntries':
            # Special handling for LogEntry list
            entries_data = payload.get('entries', [])
            payload['entries'] = [LogEntry(**entry) for entry in entries_data]
        message = message_class(**payload)
        return message
