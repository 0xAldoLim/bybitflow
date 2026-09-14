import json
from pathlib import Path

from bybit_flow.liquidity import SpreadHistory
from bybit_flow.replay import segment_rows
from bybit_flow.storage import Recorder, Store


def test_normal_spread_needs_history_and_does_not_backdate():
    history = SpreadHistory()
    for i in range(12):
        ts = (100 + i) * 300_000
        history.add(ts, ts, 99.99, 100.01)
    assert history.assess(111 * 300_000)["eligible"]
    assert history.assess(110 * 300_000)["samples"] == 11
    assert not history.assess(110 * 300_000)["eligible"]
    restored = SpreadHistory(history.export())
    assert restored.assess(111 * 300_000) == history.assess(111 * 300_000)


def test_message_frequency_cannot_multiply_quote_evidence():
    history = SpreadHistory()
    for i in range(1000):
        history.add(1000 + i, 1000 + i, 99.99, 100.01)
    assert history.assess(2000)["samples"] == 1
    assert not history.assess(2000)["eligible"]


def test_spread_tail_missing_buckets_and_stale_quotes_reject():
    history = SpreadHistory()
    for i in range(20):
        ts = (100 + i) * 300_000
        history.add(ts, ts, 99.99 if i < 15 else 99.8, 100.01 if i < 15 else 100.2)
    assert "normal spread tail exceeds gate" in history.assess(119 * 300_000)["reasons"]
    assert "normal spread observation coverage insufficient" in history.assess(140 * 300_000)["reasons"]
    stale = SpreadHistory()
    stale.add(1, 60_000, 99.99, 100.01)
    assert stale.assess(60_000)["samples"] == 0


def test_omitted_segment_becomes_explicit_replay_gap(settings):
    store = Store(settings.data_dir)
    recorder = Recorder(store, settings)
    for i in range(3):
        recorder.flush(
            [
                dict(
                    source="test",
                    symbol="T",
                    event_ms=i + 1,
                    receipt_ms=i + 1,
                    schema_version=1,
                    complete=True,
                    payload=json.dumps({"index": i}),
                )
            ]
        )
    segments = sorted(store.rows("segments"), key=lambda r: r["min_receipt_ms"])
    all_rows = list(segment_rows([Path(s["raw"]) for s in reversed(segments)]))
    assert len(all_rows) == 3
    missing = list(segment_rows([Path(segments[0]["raw"]), Path(segments[2]["raw"])]))
    assert len(missing) == 3
    assert missing[1]["source"] == "control/gap" and not missing[1]["complete"]
    store.close()


def test_minute_spread_requires_real_distinct_samples_and_recovers_from_old_gap():
    history = SpreadHistory(bucket_ms=60_000, window_ms=1_800_000)
    history.add(60_000, 60_000, 99.99, 100.01)
    for i in range(40, 52):
        ts = i * 60_000
        history.add(ts, ts, 99.99, 100.01)
        history.add(ts + 1, ts + 1, 99.99, 100.01)
    assert not history.assess(50 * 60_000)["eligible"]
    result = history.assess(51 * 60_000)
    assert result["eligible"] and result["samples"] == 12
    assert not history.assess(60 * 60_000)["eligible"]


def test_valid_later_quote_can_repair_invalid_bucket_without_backdating():
    history = SpreadHistory(bucket_ms=60_000)
    history.add(1, 60_000, 99.99, 100.01)
    history.add(70_000, 70_000, 99.99, 100.01)
    assert history.assess(65_000)["samples"] == 0
    assert history.assess(70_000)["samples"] == 1
    history.add(80_000, 80_000, 99, 101)
    assert history.export()[0]["known_ms"] == 70_000
