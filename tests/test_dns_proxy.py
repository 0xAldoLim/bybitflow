import runpy
from pathlib import Path
from unittest.mock import Mock

import pytest

relay = runpy.run_path(str(Path(__file__).parents[1] / "deploy" / "dns_proxy.py"))
QUERY = bytes.fromhex("1234010000010000000000000361706905627962697403636f6d0000010001")


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
