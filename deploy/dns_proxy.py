"""Project-local DNS wire relay over verified HTTPS; no plaintext fallback."""

import http.client
import socketserver
import ssl
import struct
import threading

SLOTS = threading.BoundedSemaphore(16)


def resolve(query):
    if len(query) < 12 or len(query) > 512 or query[2] & 0x80:
        raise ValueError("Invalid DNS query")
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
