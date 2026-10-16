import runpy
import struct
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock

import pytest

relay = runpy.run_path(str(Path(__file__).parents[1] / "deploy" / "dns_proxy.py"))
QUERY = bytes.fromhex("1234010000010000000000000361706905627962697403636f6d0000010001")


@pytest.fixture(autouse=True)
def empty_cache():
    relay["CACHE"].clear()
    yield
    relay["CACHE"].clear()


def positive_answer(query=QUERY, ttl=30):
    return (
        query[:2]
        + b"\x81\x80\x00\x01\x00\x01\x00\x00\x00\x00"
        + query[12:]
        + b"\xc0\x0c"
        + struct.pack("!HHIH", 1, 1, ttl, 4)
        + b"\x01\x02\x03\x04"
    )


def test_dns_relay_fails_closed_on_upstream_error(monkeypatch):
    client = relay["resolve"].__globals__["http"].client
    monkeypatch.setattr(client, "HTTPSConnection", Mock(side_effect=OSError("offline")))
    answer = relay["reply"](QUERY)
    assert answer[:2] == QUERY[:2]
    assert answer[3] & 15 == 2
    assert answer[6:8] == b"\x00\x00"


@pytest.mark.parametrize("query", [b"", b"x" * 513, b"\x12\x34\x80" + b"\x00" * 9])
def test_dns_relay_rejects_invalid_queries(query):
    with pytest.raises(ValueError):
        relay["resolve"](query)


def test_dns_relay_checks_transaction_id(monkeypatch):
    client = relay["resolve"].__globals__["http"].client
    response = Mock(status=200)
    response.getheader.return_value = "application/dns-message"
    response.read.return_value = b"\xab\xcd\x81\x80" + b"\x00" * 8
    connection = Mock()
    connection.getresponse.return_value = response
    monkeypatch.setattr(client, "HTTPSConnection", Mock(return_value=connection))
    with pytest.raises(ValueError, match="Invalid DNS response"):
        relay["resolve"](QUERY)
    connection.close.assert_called_once()


def test_dns_cache_rewrites_transaction_and_decrements_ttl_without_extending_it(monkeypatch):
    namespace = relay["resolve"].__globals__
    clock = [100.0]
    upstream = Mock(return_value=positive_answer(ttl=30))
    monkeypatch.setitem(namespace, "upstream", upstream)
    monkeypatch.setattr(namespace["time"], "monotonic", lambda: clock[0])
    assert relay["resolve"](QUERY) == positive_answer(ttl=30)
    clock[0] = 110
    second = b"\xab\xcd" + QUERY[2:]
    answer = relay["resolve"](second)
    assert answer[:2] == second[:2]
    assert struct.unpack_from("!I", answer, len(answer) - 10)[0] == 20
    assert upstream.call_count == 1
    clock[0] = 130
    upstream.side_effect = OSError("offline")
    failed = relay["reply"](QUERY)
    assert failed[3] & 15 == 2  # Expired cache cannot hide an upstream outage.
    assert upstream.call_count == 2


def test_concurrent_dns_lookups_share_upstream_response(monkeypatch):
    namespace = relay["resolve"].__globals__
    upstream = Mock(side_effect=lambda q: positive_answer(q))
    monkeypatch.setitem(namespace, "upstream", upstream)
    queries = [i.to_bytes(2, "big") + QUERY[2:] for i in range(16)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        answers = list(pool.map(relay["resolve"], queries))
    assert upstream.call_count == 1
    assert [a[:2] for a in answers] == [q[:2] for q in queries]


@pytest.mark.parametrize("ttl,flags", [(0, b"\x81\x80"), (30, b"\x81\x83"), (30, b"\x83\x80")])
def test_zero_ttl_negative_and_truncated_replies_are_not_cached(monkeypatch, ttl, flags):
    namespace = relay["resolve"].__globals__
    answer = positive_answer(ttl=ttl)
    upstream = Mock(return_value=answer[:2] + flags + answer[4:])
    monkeypatch.setitem(namespace, "upstream", upstream)
    relay["resolve"](QUERY)
    relay["resolve"](QUERY)
    assert upstream.call_count == 2


def test_dns_cache_is_bounded(monkeypatch):
    namespace = relay["resolve"].__globals__
    monkeypatch.setitem(namespace, "upstream", lambda q: positive_answer(q))
    for i in range(relay["CACHE_LIMIT"] + 5):
        # Change the question, not the transaction ID.
        query = QUERY[:12] + bytes([len(str(i))]) + str(i).encode() + b"\x00\x00\x01\x00\x01"
        relay["resolve"](query)
    assert len(relay["CACHE"]) == relay["CACHE_LIMIT"]
