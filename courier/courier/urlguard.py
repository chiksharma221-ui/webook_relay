"""SSRF protection.

Customers choose the URL we send requests to. Without a check, someone could
register http://169.254.169.254/ (the cloud metadata service) or
http://localhost:5432 and use Courier to reach machines inside our network.
So we refuse any URL whose host resolves to a private, loopback, link-local
or otherwise internal address, both when it's registered and again before
every delivery (DNS answers can change after registration).
"""
from __future__ import annotations

import asyncio
import ipaddress
import socket
from urllib.parse import urlsplit


class UnsafeURL(ValueError):
    pass


def _check_ip(ip: str) -> None:
    addr = ipaddress.ip_address(ip)
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped:
        addr = addr.ipv4_mapped
    if (addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_multicast
            or addr.is_reserved or addr.is_unspecified):
        raise UnsafeURL(f"address {addr} is not publicly routable")


def _parse(url: str) -> tuple[str, int]:
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https"):
        raise UnsafeURL("only http and https URLs are allowed")
    if not parts.hostname:
        raise UnsafeURL("URL has no host")
    if parts.username or parts.password:
        raise UnsafeURL("credentials in URLs are not allowed")
    return parts.hostname, parts.port or (443 if parts.scheme == "https" else 80)


def check_url(url: str, allow_private: bool = False) -> None:
    host, port = _parse(url)
    if allow_private:
        return
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        raise UnsafeURL(f"cannot resolve host {host!r}") from exc
    for info in infos:
        _check_ip(info[4][0])


async def check_url_async(url: str, allow_private: bool = False) -> None:
    host, port = _parse(url)
    if allow_private:
        return
    try:
        infos = await asyncio.get_running_loop().getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        raise UnsafeURL(f"cannot resolve host {host!r}") from exc
    for info in infos:
        _check_ip(info[4][0])
