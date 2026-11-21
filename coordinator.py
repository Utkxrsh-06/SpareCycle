import argparse
import socket

from coordinator.server import serve


def main():
    parser = argparse.ArgumentParser(description="SpareCycle LAN coordinator")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    addresses = ", ".join(socket.gethostbyname_ex(socket.gethostname())[2])
    print("Coordinator IPs:", addresses)
    print(f"Dashboard: http://localhost:{args.port}")
    serve(args.host, args.port)


if __name__ == "__main__":
    main()
