import asyncio
import logging

from raft.node import RaftNode
from raft.transport.tcp_transport import TCPTransport
from raft.storage.memory import MemoryStorage
from raft.serializer.json_serializer import JSONSerializer
from raft.state_machine.kv_store import KVStore
from raft.state import NodeState


CLUSTER = {
    "node1": ("127.0.0.1", 5001),
    "node2": ("127.0.0.1", 5002),
    "node3": ("127.0.0.1", 5003),
}


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


async def main():
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")

    # 1. Start all nodes
    print("=== Starting 3-node Raft cluster ===\n")
    nodes = {}
    for node_id, (host, port) in CLUSTER.items():
        node = create_node(node_id)
        await node.start(host, port)
        nodes[node_id] = node
        print(f"  Started {node_id} on {host}:{port}")

    # 2. Wait for leader election (poll every 100ms, timeout ~5s)
    print("\nWaiting for leader election...")
    leader_id = None
    for _ in range(50):
        await asyncio.sleep(0.1)
        for node_id, node in nodes.items():
            if node._node_state == NodeState.LEADER:
                leader_id = node_id
                break
        if leader_id:
            break

    if not leader_id:
        print("No leader elected!")
        for node in nodes.values():
            await node.stop()
        return

    print(f"Leader elected: {leader_id}\n")

    # 3. Submit commands to the leader
    print("=== Submitting commands ===\n")
    leader = nodes[leader_id]
    commands = ["SET x 10", "SET y 20", "SET z 30", "GET x", "GET y"]
    for cmd in commands:
        success, result = await leader.submit_command(cmd)
        status = "OK" if success else "FAIL"
        print(f"  {cmd:12s} => [{status}] {result}")

    # 4. Print cluster state
    await asyncio.sleep(0.3)
    print("\n--- Cluster State ---")
    for node_id, node in nodes.items():
        print(f"  {node_id}: role={node._node_state.value:9s} term={node._persistent_state.current_term}  "
              f"log={len(node._persistent_state.log)}  commit={node._volatile_state.commit_index}  "
              f"applied={node._volatile_state.last_applied}")

    # 5. Stop all nodes
    print("\n=== Shutting down ===")
    for node_id, node in nodes.items():
        await node.stop()
        print(f"  Stopped {node_id}")

    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
