import json
import logging

from bybit_flow.observability import JsonFormatter, safe_detail


def test_detail_is_one_bounded_line_without_credential_urls():
    exc = RuntimeError("Recording unavailable;\n  depth verification aborted " + "x" * 500)
    detail = safe_detail(exc)
    assert detail.startswith("Recording unavailable; depth verification aborted") and len(detail) == 200
    leaked = ValueError("POST https://discord.com/api/webhooks/123/SECRETTOKEN failed with 500")
    assert "SECRETTOKEN" not in safe_detail(leaked) and "[redacted-url]" in safe_detail(leaked)
    assert safe_detail(TimeoutError("GET https://fapi.binance.com/fapi/v1/time timed out")).endswith(
        "timed out"
    )


def test_formatter_emits_the_detail_field():
    record = logging.LogRecord("bybit_flow.scanner", logging.WARNING, __file__, 1, "scan_failed", (), None)
    record.error_type, record.detail = "RuntimeError", "Recording unavailable"
    row = json.loads(JsonFormatter().format(record))
    assert row["event"] == "scan_failed" and row["detail"] == "Recording unavailable"
