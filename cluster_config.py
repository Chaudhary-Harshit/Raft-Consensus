# This file contains the addresses of all nodes in the Raft cluster.

"""
Each node needs two TCP ports:
  1. Peer port (e.g. 5001) — for Raft messages between nodes (RequestVote, AppendEntries)
  2. Client port (e.g. 6001) — for us to send commands like SET x 10
"""

CLUSTER = {
    "node1": ("127.0.0.1", 5001),
    "node2": ("127.0.0.1", 5002),
    "node3": ("127.0.0.1", 5003),
}

CLIENT_PORT_OFFSET = 1000
