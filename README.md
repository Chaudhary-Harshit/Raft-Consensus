# Raft Consensus Algorithm

A Python implementation of the Raft consensus algorithm built from scratch using `asyncio` and TCP networking.

## What is Raft?

Raft is a consensus algorithm that allows a cluster of nodes to agree on a sequence of commands, even if some nodes fail. It ensures that all nodes apply the same commands in the same order through leader election and log replication.

## What's Implemented

### Components (pluggable via abstract base classes)
- **Transport** — `TCPTransport` using 4-byte length-prefixed binary protocol
- **Storage** — `MemoryStorage` (in-memory, volatile)
- **Serializer** — `JSONSerializer` for message serialization/deserialization
- **State Machine** — `KVStore` with SET, GET, DELETE commands

### Client Interface
- TCP-based client protocol (plain text, one command per connection)
- STATUS command to inspect node state
- Automatic leader redirect (non-leaders return the leader's ID)

## Setup

```bash
cd raft_consensus
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[test]"
```

## Running the Cluster

Start each node in a separate terminal:

```bash
# Terminal 1
python run_node.py node1

# Terminal 2
python run_node.py node2

# Terminal 3
python run_node.py node3
```

Each node logs its activity. Watch for `Became LEADER for term X` to see which node wins the election.

## Sending Commands

In a 4th terminal, connect to any node's client port (peer port + 1000):

```bash
python client.py 6001
```

Available commands:

```
> STATUS                  # Show node role, term, leader, log length
> SET mykey myvalue       # Store a key-value pair
> GET mykey               # Retrieve a value
> DELETE mykey            # Remove a key
> quit                    # Exit client
```

If you send a command to a non-leader node, it returns `ERR:<leader_id>` so you know which node to connect to.

You can add new nodes in the cluster_config.py and start the corresponding node.
