"""Resolve polymarket.com through public DNS.

The machine DNS answers these hosts with an address that never connects.
1.1.1.1 and 8.8.8.8 return the real address. Other hosts keep the system DNS.
"""

from __future__ import annotations

import socket
import struct
from typing import Optional

_REAL_GETADDRINFO = socket.getaddrinfo
_INSTALLED = False
_CACHE: dict[str, list[str]] = {}


def _query_a(host: str, server: str) -> list[str]:
    header = struct.pack(">HHHHHH", 0x1234, 0x0100, 1, 0, 0, 0)
    question = b"".join(bytes([len(part)]) + part.encode() for part in host.split("."))
    packet = header + question + b"\x00" + struct.pack(">HH", 1, 1)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(2.0)
    try:
        sock.sendto(packet, (server, 53))
        data, _ = sock.recvfrom(512)
    finally:
        sock.close()
    if len(data) < 12:
        return []
    offset = 12
    while offset < len(data) and data[offset] != 0:
        offset += data[offset] + 1
    offset += 5
    answers = struct.unpack(">H", data[6:8])[0]
    found: list[str] = []
    for _ in range(answers):
        if offset + 12 > len(data):
            break
        if data[offset] & 0xC0 == 0xC0:
            offset += 2
        else:
            while offset < len(data) and data[offset] != 0:
                offset += data[offset] + 1
            offset += 1
        typ, _clas, _ttl, length = struct.unpack(">HHIH", data[offset:offset + 10])
        offset += 10
        if typ == 1 and length == 4 and offset + 4 <= len(data):
            found.append(".".join(str(part) for part in data[offset:offset + 4]))
        offset += length
    return found


def public_addresses(host: str) -> list[str]:
    cached = _CACHE.get(host)
    if cached:
        return cached
    for server in ("1.1.1.1", "8.8.8.8"):
        try:
            found = _query_a(host, server)
        except Exception:
            continue
        if found:
            _CACHE[host] = found
            return found
    return []


def _needs_public_dns(host: str) -> bool:
    text = host.lower().rstrip(".")
    return text == "polymarket.com" or text.endswith(".polymarket.com")


def _getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    if isinstance(host, str) and _needs_public_dns(host):
        addresses = public_addresses(host)
        if addresses:
            port_n = int(port)
            return [
                (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (ip, port_n))
                for ip in addresses
            ]
    return _REAL_GETADDRINFO(host, port, family, type, proto, flags)


def install() -> None:
    """Use public DNS for polymarket.com for the rest of this process."""
    global _INSTALLED
    if _INSTALLED:
        return
    socket.getaddrinfo = _getaddrinfo
    _INSTALLED = True
