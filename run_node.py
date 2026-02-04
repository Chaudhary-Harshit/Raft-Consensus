import asyncio
import logging
import sys

from raft.node import RaftNode
from raft.transport.tcp_transport import TCPTransport
from raft.storage.memory import MemoryStorage
from raft.serializer.json_serializer import JSONSerializer
from raft.state_machine.kv_store import KVStore
from cluster_config import CLIENT_PORT_OFFSET, CLUSTER


def create_node(node_id: str) -> RaftNode:
    peers = {
        nid: f"{host}:{port}"
        for nid, (host, port) in CLUSTER.items()
        if nid != node_id
    }
    return RaftNode(
        node_id=node_id,
        peers=peers,
        transport=TCPTransport(),
        storage=MemoryStorage(),
        serializer=JSONSerializer(),
        state_machine=KVStore(),
    )


async def handle_client_connection(reader, writer, node):
    try:
        line = await reader.readline()
        if not line:
            return
        command = line.decode().strip()

        if command.upper() == "STATUS":
            response = (
                f"id={node._node_id} role={node._node_state.value} "
                f"term={node._persistent_state.current_term} leader={node._leader_id} "
                f"log={len(node._persistent_state.log)} commit={node._volatile_state.commit_index}\n"
            )
        else:
            success, result = await node.submit_command(command)
            if success:
                response = f"OK:{result}\n"
            else:
                response = f"ERR:{result}\n"

        writer.write(response.encode())
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    if len(sys.argv) != 2 or sys.argv[1] not in CLUSTER:
        print(f"Usage: python run_node.py <{'|'.join(CLUSTER.keys())}>")
        sys.exit(1)

    node_id = sys.argv[1]
    host, peer_port = CLUSTER[node_id]
    client_port = peer_port + CLIENT_PORT_OFFSET

    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [{node_id}] [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    node = create_node(node_id)
    await node.start(host, peer_port)
    logging.info(f"Raft peer port: {peer_port}")

    client_server = await asyncio.start_server(
        lambda r, w: handle_client_connection(r, w, node),
        host, client_port,
    )
    logging.info(f"Client port: {client_port}")
    logging.info("Ready. Ctrl+C to stop.")

    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        logging.info("Shutting down...")
        client_server.close()
        await client_server.wait_closed()
        await node.stop()
        logging.info("Stopped.")


if __name__ == "__main__":
    asyncio.run(main())
