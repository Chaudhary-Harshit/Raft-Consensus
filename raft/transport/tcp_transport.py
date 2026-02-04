import asyncio
import logging
import struct
from raft.transport.base import TransportBase
from typing import Awaitable, Callable

logger = logging.getLogger(__name__)


class TCPTransport(TransportBase):
    def __init__(self):
        self._host = None
        self._port = None
        self._server = None
        self._handler = None

    def register_handler(self, handler: Callable[[str, bytes], Awaitable[None]]) -> None:
        """
        The handler is called for each incoming message with the sender's address and the message bytes.
        This will be called when the RaftNode would be setting up the transport layer. And a function from RaftNode would be passed here.
        Transport layer is only concerned about receiving messages and passing them to the handler. The processing of the message is the responsibility of the RaftNode
        """
        self._handler = handler

    async def start(self, host: str, port: int) -> None:
        self._host = host
        self._port = port
        self._server = await asyncio.start_server(self._handle_connection, host, port)  # we are only passing the handler here, it will be called when each new connection is made
        await self._server.start_serving()

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handle incoming connections and delegate to the registered handler. Called for each new connection."""

        addr = writer.get_extra_info('peername')  # tihis returns a tuple (host, port) like ('127.0.0.1', 54321)
        sender_address = f"{addr[0]}:{addr[1]}"

        try:
            length_prefix = await reader.readexactly(4)  # read exactly 4-byte length prefix
            message_length = struct.unpack(">I", length_prefix)[0]  # unpack the length prefix to get the message length (as unpackr returns a tuple, we take the first element)

            message = await reader.readexactly(message_length)  # read the actual message based on the length
            logger.debug(f"Received {message_length} bytes from {sender_address}")
            if self._handler:
                await self._handler(sender_address, message)
        except asyncio.IncompleteReadError:
            logger.warning(f"Incomplete read from {sender_address}")
        except Exception as e:
            logger.error(f"Error handling connection from {sender_address}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()  # stop accepting new connections, initiate server shutdown
        await self._server.wait_closed()  # wait until the server is fully closed and initiate closing existing connections
        self._server = None

    async def send(self, to: str, message: bytes) -> None:
        host, port_str = to.rsplit(":", 1)
        port = int(port_str)
        try:
            _, writer = await asyncio.open_connection(host, port)
        except Exception as e:
            print(f"[TCP] CONNECT FAILED to {to}: {e}")
            raise
        length_prefix = struct.pack(">I", len(message))  # 4-byte big-endian length prefix , so that the receiver knows how many bytes to read
        writer.write(length_prefix)
        writer.write(message)
        await writer.drain()
        writer.close()
        await writer.wait_closed()
