# Upgrade verification — 16 September 2026

This report records an in-place upgrade of the existing Windows installation. Values below are a verification snapshot, not a promise of future availability or trading performance.

## Verification status

| Check | Result |
|---|---|
| Latest commit | The commit containing this report; see `git log -1` |
| Test count | 186 tests; full pytest and Ruff validation |
| Docker | Desk and trainer images built; existing persistent volume retained |
| Scanner | Live collection and recording restored; dashboard health HTTP 200 |
| Supported horizons | SHORT_INTRADAY, CORE_INTRADAY, SWING; EXTENDED_SWING shadow only; LEGACY preserved |
| SHORT_INTRADAY | 1H / 15M / 5M context, setup, execution; 15 minutes–2 hours |
| CORE_INTRADAY | 4H / 1H / 15M; 1–4 hours |
| SWING | 1D / 4H / 1H; 4–48 hours |
| EXTENDED_SWING | 1D / 4H / 1H; 2–7 days; no live recommendation |
| Sessions | IANA time zones and daylight-saving-aware session context |
| Combined score | One quality score; SSS 95, SS 90, S 85, A 75, B 65, C 50, D 35, E 20, F below 20 or mandatory rejection |
| Order flow | Real executed trades, delta/CVD, persistence and book pressure; depth reconnects no longer erase a healthy separate Binance trade stream |
| Auction model | Retained executed-volume value/POC, range acceptance/rejection and auction context; unavailable coverage stays unavailable |
| BTC/ETH beta | Causal rolling beta, correlation and residual context; no future-bar fit |
| Execution model | Public quote/print observation and hypothetical costs, not account fills; fresh book required for qualification |
| ML | Automatic worker enabled; no champion or trained model yet; research alerts are not blocked by ML readiness |
| Horizon model | Empirical research recommendation after sufficient coverage-complete observations; not a validated learned horizon classifier |
| Post-terminal tracking | Lightweight observations continue after operational expiry; original primary outcomes immutable |
| Late-outcome labels | Separate research labels; no mature late labels at verification |
| Storage auto cleanup | Enabled; protects active plans, unresolved outcomes and replay leases; preserves permanent learning and audit records |
| Current storage | Approximately 6.00 GB managed data, below the 10 GB cap; 8.66 GB freed by verified cleanup |
| Discord test | Real webhook connection test returned sent |
| Test signal | Real webhook accepted explicitly synthetic CORE_INTRADAY test; zero signal/ML rows created |
| Real market connectivity | Binance, Bybit and OKX: healthy REST and genuine fresh trade WebSocket messages |
| Active setups before | 3 at initial backup; 20 at migration backup |
| Active setups after | Zero at the final restart snapshot; new scan warming up. Earlier warmup produced six pending confirmations; no new real initial alert delivered in this verification window |
| Legacy active restored | Plans preserved; all baseline deadlines passed and records correctly expired; no expired trade reactivated |
| Duplicate signals created | Migration preserves IDs; terminal-record hashes unchanged; no synthetic signal rows |
| SSS research | Enabled as an uncalibrated quality tier when mandatory gates pass |
| Validated SSS | Not established; no validated champion or proven win probability |

The full suite covers deterministic/mocked recovery and lifecycle behavior. Live exchange probes and webhook responses are separate external checks. A delivered synthetic card is not a real qualifying trade or evidence of profitable performance. The in-app browser blocked localhost navigation; endpoint checks and JavaScript syntax checks passed, but no visual-browser QA is claimed.

After the final feed-recovery deployment, the live Binance one-minute API returned complete/available flow for BTCUSDT (823 trades, +29.10% delta, CVD +49.932 BTC) and DOGEUSDT (83 trades, +4.29% delta). These are observed diagnostic windows, not trading recommendations. Desk and DNS container health checks passed; the trainer remained running.

## Existing signal continuity

Two independently hashed SQLite backups were retained outside the managed data volume. All 10,174 initially terminal signals and all 10,363 terminal signals at migration retain identical payload hashes: none removed or changed. Entry, stop, targets, original quality/tier, version and horizon of every baseline active setup remained unchanged. Monitoring evaluated elapsed deadlines; expiry is not a win or a new recommendation.

| Baseline | Signal ID | Symbol | Direction | Before | After | Creation score | Creation tier | Horizon | Monitoring restored |
|---|---|---|---|---|---|---|---|---|---|
| upgrade-before | 579b40a2a1c8c44910497508 | TAOUSDT | LONG | ALERTED | EXPIRED | 69.6 | B | LEGACY | Yes; expired at frozen deadline |
| upgrade-before | b9e247004e5deb4fcecd4e47 | ARBUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 0.0 | F | LEGACY | Yes; expired at frozen deadline |
| upgrade-before | 1905bef735a6cbbf1d4dc019 | ARBUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 0.0 | F | LEGACY | Yes; expired at frozen deadline |
| migration-baseline | 5f75787e2980a8bb22d6256b | DOGEUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 52.7 | F | LEGACY | Yes; expired at frozen deadline |
| migration-baseline | 650527a683037ebd9ec89cdc | SOLUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 56.0 | F | LEGACY | Yes; expired at frozen deadline |
| migration-baseline | 2ed53687268f201b1530f54e | SOLUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 56.0 | F | LEGACY | Yes; expired at frozen deadline |
| migration-baseline | 766518991422579cf358f92d | ARBUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 50.0 | F | LEGACY | Yes; expired at frozen deadline |
| migration-baseline | 63a841bd725ea065885d1839 | ARBUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 50.0 | F | LEGACY | Yes; expired at frozen deadline |
| migration-baseline | 22aa05e48e57cfbed7f3aa8c | APTUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 48.8 | F | LEGACY | Yes; expired at frozen deadline |
| migration-baseline | 9b4975b454ac8a472c3773e3 | DOGEUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 52.7 | F | LEGACY | Yes; expired at frozen deadline |
| migration-baseline | 1edd73fa138d2bf8b29a68ff | SOLUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 56.0 | F | LEGACY | Yes; expired at frozen deadline |
| migration-baseline | d529eaf649a5501d422bc805 | SOLUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 56.0 | F | LEGACY | Yes; expired at frozen deadline |
| migration-baseline | 8f788d3e28507284bcea7599 | ARBUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 50.0 | F | LEGACY | Yes; expired at frozen deadline |
| migration-baseline | ecfe4470b716cb5db83a4977 | ARBUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 50.0 | F | LEGACY | Yes; expired at frozen deadline |
| migration-baseline | ac0a08e2b63f65dc5df697f7 | APTUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 48.8 | F | LEGACY | Yes; expired at frozen deadline |
| migration-baseline | 9aabc61ffee024dc37530a44 | ETHUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 0.0 | F | LEGACY | Yes; expired at frozen deadline |
| migration-baseline | 7aa81d2b6ae9852dd58e8576 | ETHUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 0.0 | F | LEGACY | Yes; expired at frozen deadline |
| migration-baseline | 71835f2f7e61f3459eaa59d0 | BTCUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 0.0 | F | LEGACY | Yes; expired at frozen deadline |
| migration-baseline | 85cfb01f4d9916957442125c | BTCUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 0.0 | F | LEGACY | Yes; expired at frozen deadline |
| migration-baseline | 76b032feac4d881db5ad7de2 | ETHUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 0.0 | F | LEGACY | Yes; expired at frozen deadline |
| migration-baseline | 3863541ddbd792c350c2abd7 | ETHUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 0.0 | F | LEGACY | Yes; expired at frozen deadline |
| migration-baseline | 55631a8446cabb71ac77cbb2 | BTCUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 0.0 | F | LEGACY | Yes; expired at frozen deadline |
| migration-baseline | 4435b9863dbced7098fc096f | BTCUSDT | LONG | PENDING CONFIRMATION | EXPIRED | 0.0 | F | LEGACY | Yes; expired at frozen deadline |

## Multi-horizon support and selection

The scanner evaluates enabled profiles with their own closed-candle context/setup/execution frames, shared live feeds, horizon-specific risk and holding deadlines. Distinct qualified plans can publish independently. Near-identical theses remain deduplicated. Existing LEGACY plans retain their original rules. EXTENDED_SWING remains research-only.

Deep collection supports 30 symbols. Core symbols and eligible high-ranking candidates share capacity with approximately 10–15% deterministic exploration; exploration never bypasses mandatory liquidity, coverage or risk checks. Selection metadata preserves ranking/probability context. A high score is not a calibrated probability. Lower tiers may publish when their setup and executed-flow confirmation pass; incomplete evidence is not manufactured to increase alert volume.

Price structure, executed flow, auction/value, derivatives, session context, BTC/ETH residuals and stop/noise context contribute to the same score. Book pressure includes touch and 5/10/25-bps bands with persistence rather than treating a single frame as proof. Separate depth recovery invalidates stale books and book frames while preserving continuous trades; actual trade disconnections still reset coverage.

## What happens after tracking ends

Operational expiry, invalidation or resolution ends the active recommendation. The original primary outcome is frozen. Lightweight candle observations can continue at configured horizon checkpoints. Later target movement, directional afterlife and extended same-rules outcomes are separate research records. A stop before a later target remains a stopped path; incomplete coverage stays unknown. Late movement never rewrites the original trade into a win.

Decision-time feature snapshots exclude future outcomes. ML can later study timing/horizon errors through separately labelled research data; current empirical horizon recommendations require sufficient complete observations and are not a trained production classifier.

## ML readiness

The recovery pass retained 8,498 outcome labels. Only 34 were complete, and 13 qualified as complete outcomes with 16-observation sequences at the live readiness check. The two-stage pipeline requires at least 500 such outcomes across chronological training, calibration, validation and holdout partitions. Incomplete/censored outcomes remain stored for audit and do not count as completed training examples.

Stage one compares LightGBM, Random Forest and LSTM; stage two compares Logistic Regression, SVM and Random Forest probability outputs. The worker rechecks readiness every 15 minutes; successful fitting remains on a weekly cadence. No model is automatically promoted. Research signals continue using deterministic metrics while probabilities remain uncalibrated. Current models: zero. Availability of the algorithms does not establish validated trading performance.

## Storage and recovery

The completed recovery process exited successfully. Managed data fell from 14,662,124,358 bytes to 5,999,966,669 bytes, freeing 8,662,157,689 bytes. Subsequent recording increases usage normally. The configured budget remains 10,000,000,000 bytes.

Permanent signal history, decision snapshots, outcome labels, models and hashes remain. Old raw/Parquet recording files are removed only beyond protected active/unresolved/replay boundaries. Lightweight post-terminal records remain. Safe cleanup begins under pressure and aims for 60% capacity; active evidence can block deletion, in which case recording stops rather than discarding required evidence. Lossless packing is available separately with hash verification.

Two verified rollback backups consume an additional 3,825,045,504 bytes on the Windows host **outside** the managed 10 GB application cap. Docker images, Docker's virtual disk allocation and host backups are also outside that cap. The cap does not mean total Docker disk usage is 10 GB.

Docker's stale runtime socket directories were preserved and recreated to restore startup. No factory reset, volume deletion or WSL unregister was performed. TLS verification remains enabled. Exchange tests used the existing container DNS relay; this upgrade did not change global Windows DNS settings.

## Discord status and operation

The webhook is configured. The connection test and clearly marked synthetic setup both returned `sent`. Synthetic testing created no trade or learning records. Persistent outbox identities and original signal IDs remain; ambiguous historical sends are not retried blindly. There were eight historical uncertain outbox records at verification, retained for audit. The latest connection and synthetic sends succeeded. The existing TAOUSDT lifecycle showed delivered pause, resume and tracking-ended updates. No new real initial trade alert was delivered during this verification window. No current notification error was recorded.

New cards identify horizon/holding period, entry zone, stop, targets, timing, quality and concise setup context. Feed interruptions pause monitoring instead of immediately withdrawing a published plan. A real stop or expired frozen deadline still ends the recommendation. Publication requires fresh qualifying evidence; there is no guaranteed signal arrival time.

From Windows CMD:

```bat
cd /d C:\Users\USER\Documents\Codex\bybitflow
rem Start scanner and automatic ML worker
docker compose --profile ml up -d
rem Check services
docker compose --profile ml ps
rem Stop without deleting data
docker compose --profile ml stop
```

Keep Docker Desktop running and Windows awake for collection. Use the same start command after shutdown. See [the operator guide](USER_RUNBOOK.md) for settings, diagnostics and cleanup commands.

## Remaining limitations

Exchange REST timeouts and occasional depth/DNS reconnects remain possible; the services retry and only qualify fresh evidence. Depth recovery preserves the separate trade feed, but genuine trade gaps still require a new continuous window. New live alerts and empirical late-label learning need runtime and qualifying market conditions. This verification does not establish profitable performance, a trained horizon classifier, completed day/week flow coverage, or account execution accuracy.
