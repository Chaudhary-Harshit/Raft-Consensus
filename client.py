import asyncio
import sys


async def send_command(host, port, command):
    reader, writer = await asyncio.open_connection(host, port)
    writer.write((command + "\n").encode())  # Writes in the buffer data in memory
    await writer.drain()  # Flushes the buffer and sents it over the network
    response = await reader.readline()
    writer.close()  # Initiates a graceful TCP shutdown (sends a FIN packet)
    await writer.wait_closed()  # This waits for the OS to complete the shutdown, close the socket
    return response.decode().strip()


async def main():
    if len(sys.argv) != 2:
        print("Usage: python client.py <port>")
        sys.exit(1)

    port = int(sys.argv[1])
    host = "127.0.0.1"

    print(f"Connected to {host}:{port}")
    print("Commands: SET <key> <value> | GET <key> | DELETE <key> | STATUS | quit\n")

    while True:
        command = input("> ").strip()
        if not command:
            continue
        if command.lower() == "quit":
            break
        try:
            response = await send_command(host, port, command)
            print(response)
        except ConnectionRefusedError:
            print("ERROR: Could not connect to node")
        except Exception as e:
            print(f"ERROR: {e}")


if __name__ == "__main__":
    asyncio.run(main())
