"""Fixed production fixture rendered identically by 2.2.2 (golden) and later releases.

Regenerate only for an intentional, reviewed production change:
    python tests/production_golden.py > tests/fixtures/production_v222_golden.json
"""

import json
import sys
from pathlib import Path

from test_setup_v22 import NOW, tagged

from bybit_flow.config import Settings
from bybit_flow.models import Instrument, Signal
from bybit_flow.notifications import embed
from bybit_flow.quality_v22 import apply
from bybit_flow.storage import Store

GOLDEN = Path(__file__).parent / "fixtures" / "production_v222_golden.json"
INSTRUMENT = {
    "symbol": "TESTUSDT",
    "baseCoin": "TEST",
    "settleCoin": "USDT",
    "quoteCoin": "USDT",
    "contractType": "LinearPerpetual",
    "status": "Trading",
    "launchTime": "1000",
    "isPreListing": False,
    "fundingInterval": 480,
    "priceFilter": {"tickSize": "0.01"},
    "lotSizeFilter": {
        "qtyStep": "0.001",
        "minOrderQty": "0.001",
        "minNotionalValue": "5",
        "maxMktOrderQty": "100000",
    },
    "leverageFilter": {"maxLeverage": "50"},
}


def fixture_signal(ident):
    return Signal(
        id=ident,
        symbol="TESTUSDT",
        direction="LONG",
        family="trend_pullback",
        created_ms=1000,
        expires_ms=3_601_000,
        regime="trending up",
        entry=100,
        zone=(99, 101),
        stop=95,
        tp1=115,
        tp2=120,
        invalidation="Price below 95",
        reason="Synthetic test fixture, not market data",
    )


def production_view(root):
    settings = Settings(_env_file=None, data_dir=Path(root), rest_requests_per_second=10)
    instrument = Instrument.parse(INSTRUMENT)
    store = Store(settings.data_dir)
    from bybit_flow.forward_evidence import initialize

    initialize(store, 0)
    try:
        from bybit_flow.forward_power import initialize as initialize_power

        initialize_power(store, 0, settings)
    except ImportError:
        pass  # The pre-release golden has no evidence-power module.
    view = {}
    for case, flow in (("confirmed", True), ("flow_failed", False)):
        s, book = tagged(fixture_signal("golden-" + case), settings, instrument)
        s.evidence["flow_confirmed"] = flow
        if not flow:
            s.gates = ["executed order flow did not confirm family trigger"]
        apply(s, 100.01, NOW, minimum_rr=settings.min_net_rr, book=book, settings=settings)
        store.signal(s)
        saved = json.loads(store.db.execute("SELECT payload FROM signals WHERE id=?", (s.id,)).fetchone()[0])
        view[case] = dict(
            quality=saved["quality"],
            gates=saved["gates"],
            raw_tier=saved["raw_tier"],
            final_tier=saved["final_tier"],
            plan=[saved[k] for k in ("entry", "stop", "tp1", "tp2", "zone")],
            risk=saved["risk"],
            setup_assessment=saved["evidence"].get("v22"),
            discord=embed(s, settings.dashboard_url),
        )
    store.close()
    return json.dumps(view, sort_keys=True, indent=1)


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory(dir=sys.argv[1] if len(sys.argv) > 1 else None) as root:
        print(production_view(root))
