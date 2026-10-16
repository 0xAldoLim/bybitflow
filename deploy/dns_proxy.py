"""Project-local DNS wire relay over verified HTTPS; no plaintext fallback."""

import http.client
import socketserver
import ssl
import struct
import threading
import time
from collections import OrderedDict

SLOTS = threading.BoundedSemaphore(16)
CACHE = OrderedDict()
CACHE_LOCK = threading.Lock()
QUERY_LOCKS = [threading.Lock() for _ in range(32)]
CACHE_LIMIT = 128


def skip_name(message, offset):
    while offset < len(message):
        size = message[offset]
        offset += 1
        if size == 0:
            return offset
        if size & 0xC0 == 0xC0 and offset < len(message):
            if ((size & 0x3F) << 8 | message[offset]) >= len(message):
                break
            return offset + 1
        if size & 0xC0 or offset + size > len(message):
            break
        offset += size
    raise ValueError("Invalid DNS name")


def cache_fields(answer):
    """Cache only complete positive replies; every ordinary RR keeps its own TTL."""
    if answer[2] & 2 or answer[3] & 15:
        return []
    questions, answers, authority, additional = struct.unpack_from("!HHHH", answer, 4)
    if questions != 1 or not answers:
        return []
    offset = skip_name(answer, 12) + 4
    fields = []
    for _ in range(answers + authority + additional):
        offset = skip_name(answer, offset)
        kind, _cls, ttl, size = struct.unpack_from("!HHIH", answer, offset)
        if kind != 41:  # OPT's TTL position contains EDNS flags, not a cache lifetime.
            fields.append((offset + 4, ttl))
        offset += 10 + size
        if offset > len(answer):
            raise ValueError("Invalid DNS record")
    if offset != len(answer):
        raise ValueError("Invalid DNS record bounds")
    return fields


def cached_answer(key, transaction):
    with CACHE_LOCK:
        cached = CACHE.get(key)
        if cached is None:
            return None
        answer, fields, saved, expires = cached
        now = time.monotonic()
        if now >= expires:
            del CACHE[key]
            return None
        CACHE.move_to_end(key)
        remaining = bytearray(answer)
        remaining[:2] = transaction
        elapsed = max(0, int(now - saved))
        for offset, ttl in fields:
            struct.pack_into("!I", remaining, offset, max(0, ttl - elapsed))
        return bytes(remaining)


def resolve(query):
    if len(query) < 12 or len(query) > 512 or query[2] & 0x80:
        raise ValueError("Invalid DNS query")
    key = query[2:]
    # Concurrent identical lookups share one verified upstream response. Separate
    # hash stripes avoid blocking unrelated names behind a slow upstream request.
    with QUERY_LOCKS[hash(key) % len(QUERY_LOCKS)]:
        cached = cached_answer(key, query[:2])
        if cached is not None:
            return cached
        answer = upstream(query)
        try:
            fields = cache_fields(answer)
        except (ValueError, struct.error):
            fields = []
        lifetime = min((ttl for _, ttl in fields), default=0)
        if lifetime > 0:
            now = time.monotonic()
            with CACHE_LOCK:
                CACHE[key] = (answer, fields, now, now + min(60, lifetime))
                CACHE.move_to_end(key)
                while len(CACHE) > CACHE_LIMIT:
                    CACHE.popitem(last=False)
        return answer


def upstream(query):
    with SLOTS:
        connection = http.client.HTTPSConnection(
            "dns.google", timeout=8, context=ssl.create_default_context()
        )
        try:
            connection.request(
                "POST",
                "/dns-query",
                query,
                {"Content-Type": "application/dns-message", "Accept": "application/dns-message"},
            )
            response = connection.getresponse()
            answer = response.read(65536)
            if (
                response.status != 200
                or response.getheader("Content-Type", "").split(";")[0] != "application/dns-message"
            ):
                raise ValueError("Encrypted DNS upstream unavailable")
            if not 12 <= len(answer) <= 65535 or answer[:2] != query[:2] or not answer[2] & 0x80:
                raise ValueError("Invalid DNS response")
            return answer
        finally:
            connection.close()


def reply(query):
    try:
        return resolve(query)
    except Exception:
        # SERVFAIL, retaining the transaction ID but no fabricated answer.
        return query[:2] + b"\x81\x82" + b"\x00" * 8 if len(query) >= 12 else b""


class UDPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        query, sock = self.request
        answer = reply(query)
        if answer:
            if len(answer) > 512:
                # Require TCP rather than sending oversized legacy UDP responses.
                answer = answer[:2] + bytes([answer[2] | 2, answer[3]]) + b"\x00" * 8
            sock.sendto(answer, self.client_address)


class TCPHandler(socketserver.StreamRequestHandler):
    def handle(self):
        self.request.settimeout(10)
        size = self.rfile.read(2)
        if len(size) != 2:
            return
        length = struct.unpack("!H", size)[0]
        if not 12 <= length <= 512:
            return
        answer = reply(self.rfile.read(length))
        if answer:
            self.wfile.write(struct.pack("!H", len(answer)) + answer)


class UDPServer(socketserver.ThreadingUDPServer):
    daemon_threads = True


class TCPServer(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True


if __name__ == "__main__":
    with TCPServer(("0.0.0.0", 53), TCPHandler) as tcp, UDPServer(("0.0.0.0", 53), UDPHandler) as udp:
        threading.Thread(target=tcp.serve_forever, daemon=True).start()
        udp.serve_forever()
