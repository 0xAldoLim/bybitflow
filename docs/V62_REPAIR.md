# Live lifecycle repair

Active plans restore their original-source subscriptions before broad scanning.
An independent one-second worker follows executed trades for entry, stop and
target touches. Gaps pause monitoring until original-source one-minute candles
cover the missing interval and the live tape is fresh. Ambiguous candles use the
existing conservative stop-first rule. Other-source plans remain explicitly
paused for original-source reconciliation; their price paths are never mixed.

Terminal state and a durable terminal event are committed before Discord I/O.
The delivery worker retries edits to the original message with bounded backoff.
It does not post duplicate withdrawal cards. Missing original message IDs and
permanent HTTP failures are reported as failed delivery. Unpublished setups do
not generate withdrawal messages.

New intraday plans use structural anchors with volatility and wick-aware stop
buffers. Unsafe distances are rejected rather than compressed. WICKY short
intraday reversals require closed five-minute reclaim and hold/retest, supportive
flow, participation and profile evidence. Entry checks reject exhausted reward
or excessive displacement. Existing plan prices, scores and versions are not
migrated. Swing structural stop construction is unchanged.

Candidate creation waits for a complete retained flow window. Coverage counters
separate evaluations from unique missing windows. Doctor reports stream,
recorder, lifecycle, thesis-health, coverage and terminal-delivery status
separately. A recent heartbeat alone cannot qualify the runtime as fresh.

Run from the repository folder:

```cmd
docker compose --profile ml up -d --build
docker compose exec desk bybit-flow doctor
docker compose exec desk bybit-flow signals status
```

Stop while preserving stored research and learning data:

```cmd
docker compose --profile ml stop
```

Restart with `docker compose --profile ml up -d`. Candle reconciliation cannot
recover tick ordering or prove account fills. Secondary venue collection is
bounded to eight candidate/active symbols per venue; unavailable comparison is
explicit and never inferred from the primary venue.
