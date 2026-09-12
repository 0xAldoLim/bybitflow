# Reviewed asset facts

Asset facts provide source-attributed context for quality scoring. Coverage of
economic purpose, value accrual, dilution, security, and governance can earn points.
Missing categories earn no points. Coverage is not a valuation or incident clearance.

## Review fields

Each fact contains an asset identifier, category, definition, observation, source,
knowledge time, effective time, collection time, and expiry. Historical candidate
scores are not rewritten when a fact is added. Known major events can block setups.

Record reviewed facts through the Research page or authenticated `POST /api/facts`.
Set knowledge time to when the information was reviewed; do not backdate imports.
Renew expiry only after reviewing the source again.

## Reference pack

`examples/reviewed-assets-20260911.json` contains 94 baseline entries covering the
30 numbered assets in the [CoinGecko market-cap listing](https://www.coingecko.com/en/all-cryptocurrencies)
reviewed on 11 September 2026. Each entry includes its primary source. Unnumbered
wrapped and staked duplicates are excluded.

BTC, ETH, USDT, BNB, XRP, USDC, SOL, TRX, FIGR_HELOC, ZEC, HYPE, DOGE, RAIN,
USDS, XMR, WBT, LINK, LEO, ADA, XLM, DAI, BCH, USDE, USD1, LTC, CC, UNI,
GRAM, USDG, HBAR.

The pack is a dated reference, not an automatic import or a current market-cap feed.
Coverage varies by asset. Reserves, unlocks, incidents, and deployment-specific
contract mappings may require additional review.

[TON's official media documentation](https://ton.org/media/) identifies Gram/GRAM
as the token formerly named Toncoin/TON. Exchange symbol mappings remain
source-specific.

## Market coverage

`examples/ml-top30.env` prioritizes supported non-stablecoin pairs and sets deep
capacity to 30. Discovery, metadata, liquidity, and coverage checks still determine
actual subscriptions. Assets without supported perpetual markets remain research
context. The wider scanner remains available.
