import json

from bybit_flow.ml.labels import label_recordings
from bybit_flow.ml.store import FeatureStore
from bybit_flow.storage import Store


def trade(at, price):
    return dict(
        source="ws/publicTrade.TESTUSDT",
        symbol="TESTUSDT",
        event_ms=at,
        receipt_ms=at,
        complete=True,
        payload=json.dumps({"data": [dict(T=at, p=str(price), v="1000", i=str(at), S="Buy")]}),
    )


def test_incremental_restart_matches_full_replay(settings, signal, tmp_path):
    rows = [
        dict(
            source="control/subscribed",
            symbol="TESTUSDT",
            event_ms=500,
            receipt_ms=500,
            complete=True,
            payload=json.dumps({"symbols": ["TESTUSDT"]}),
        ),
        trade(1500, 100),
        trade(5000, 116),
    ]
    first, second = Store(tmp_path / "full"), Store(tmp_path / "incremental")
    for store in (first, second):
        FeatureStore(store).capture(signal, 1000, "decision")
    expected = label_recordings(first, rows, settings)["outcomes"][0]
    label_recordings(second, rows[:2], settings, incremental=True)
    checkpoint = second.get("primary_materialization")
    assert checkpoint["cursor_ms"] == 1500 and checkpoint["pending"] == 1
    second.close()
    second = Store(tmp_path / "incremental")
    result = label_recordings(second, rows[2:], settings, incremental=True)["outcomes"][0]
    for key in (
        "classification",
        "net_r",
        "quantity",
        "actual_entry_ms",
        "exit_ms",
        "fees",
        "data_gaps",
        "complete",
    ):
        assert result[key] == expected[key]
    assert second.get("primary_materialization")["pending"] == 0
    label_recordings(second, rows, settings, incremental=True)
    assert second.db.execute("SELECT count(*) FROM ml_labels").fetchone()[0] == 1
    first.close()
    second.close()


def test_gapped_long_horizon_is_excluded_without_waiting_and_active_plan_survives(settings, signal):
    store = Store(settings.data_dir)
    signal.expected_hold_max = 10080
    signal.horizon_profile = "EXTENDED_SWING"
    signal.holding_deadline_ms = 1000 + 10080 * 60_000
    signal.state = "ALERTED"
    store.signal(signal)
    ident = FeatureStore(store).capture(signal, 1000, "decision")
    rows = [
        dict(
            source="control/subscribed",
            symbol=signal.symbol,
            event_ms=500,
            receipt_ms=500,
            complete=True,
            payload=json.dumps({"symbols": [signal.symbol]}),
        ),
        trade(1500, 100),
    ]
    label_recordings(store, rows, settings, incremental=True)
    assert store.get("primary_materialization")["pending"] == 1
    assert not store.db.execute("SELECT 1 FROM ml_labels WHERE snapshot_id=?", (ident,)).fetchone()
    label_recordings(
        store,
        [
            dict(
                source="control/gap",
                symbol=signal.symbol,
                event_ms=2000,
                receipt_ms=2000,
                complete=False,
                payload=json.dumps({"reason": "connection lost"}),
            ),
        ],
        settings,
        incremental=True,
    )
    label = json.loads(
        store.db.execute("SELECT payload FROM ml_labels WHERE snapshot_id=?", (ident,)).fetchone()[0]
    )
    assert label["classification"] == "incomplete" and not label["complete"]
    assert label["net_r"] is None
    assert store.get("primary_materialization")["pending"] == 0
    assert store.active_signals()[0]["state"] == "ALERTED"
    store.close()
