"""Length-prefixed JSON protocol used by workers and the coordinator."""

import hashlib
import json
import socket
import struct
import uuid

MAX_MESSAGE = 64 * 1024 * 1024


def checksum(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def message(kind: str, **data):
    return {"id": str(uuid.uuid4()), "type": kind, **data}


def send(sock: socket.socket, payload: dict):
    raw = json.dumps(payload, separators=(",", ":")).encode()
    if len(raw) > MAX_MESSAGE:
        raise ValueError("message exceeds protocol limit")
    sock.sendall(struct.pack("!I", len(raw)) + raw)


def _exact(sock, size):
    out = bytearray()
    while len(out) < size:
        part = sock.recv(size - len(out))
        if not part:
            raise ConnectionError("peer disconnected")
        out.extend(part)
    return bytes(out)


def receive(sock: socket.socket):
    size = struct.unpack("!I", _exact(sock, 4))[0]
    if not 0 < size <= MAX_MESSAGE:
        raise ValueError("invalid message length")
    return json.loads(_exact(sock, size).decode())
