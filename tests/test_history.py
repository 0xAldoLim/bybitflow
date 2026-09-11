import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from bybit_flow.history import aggregate_trades
from bybit_flow.replay import segment_rows
from bybit_flow.storage import Recorder, Store


def test_binance_zip_normalization_is_explicit_aggregate_data(tmp_path):
    import zipfile

    from bybit_flow.history import convert_binance_archive

    raw = tmp_path / "synthetic.zip"
    with zipfile.ZipFile(raw, "w") as archive:
        archive.writestr(
            "synthetic.csv",
            "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n1,100,2,1,1,1704067200000,true\n2,101,3,2,2,1704067201000,false\n",
        )
    output = tmp_path / "synthetic.parquet"
    manifest = convert_binance_archive(raw, output, "TESTUSDT", "2024-01-01", "synthetic-software-test")
    rows = pq.read_table(output).to_pylist()
    assert manifest["rows"] == 2 and not manifest["book_coverage"]
    assert rows[0]["side"] == "Sell" and rows[1]["notional"] == "303"
    assert all(r["exchange"] == "binance" for r in rows)


def test_trade_aggregation_preserves_source_order_with_equal_timestamps(tmp_path):
    rows = [
        {"event_ms": 1000, "event_index": 0, "price": "100", "size": "2", "notional": "200"},
        {"event_ms": 1000, "event_index": 1, "price": "101", "size": "1", "notional": "101"},
        {"event_ms": 3000, "event_index": 2, "price": "99", "size": "3", "notional": "297"},
    ]
    path = tmp_path / "actual-schema-synthetic-trades.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path)
    bars = aggregate_trades(path, 1)
    assert len(bars) == 1
    assert bars[0]["open"] == 100 and bars[0]["close"] == 99
    assert bars[0]["volume"] == 6 and bars[0]["turnover"] == 598


async def test_replay_refuses_integrity_mismatch(settings):
    store = Store(settings.data_dir)
    rec = Recorder(store, settings)
    rec.offer("control/gap", "ALL", 1, {}, receipt_ms=2, complete=False)
    rec.running = False
    await rec.run()
    manifest = store.rows("segments")[0]
    from pathlib import Path

    path = Path(manifest["raw"])
    manifest_path = path.with_name(path.name.removesuffix(".jsonl.gz") + ".manifest.json")
    metadata = json.loads(manifest_path.read_text())
    metadata["sha256"] = hashlib.sha256(b"wrong file").hexdigest()
    manifest_path.write_text(json.dumps(metadata))
    with pytest.raises(ValueError, match="integrity mismatch"):
        list(segment_rows([path]))
    store.close()
