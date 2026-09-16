# Research rationale

These materials motivate testable hypotheses and feature engineering. They do
not establish BybitFlow profitability. The original PDFs are not bundled; the
implementation uses the supplied principle summaries, not claims of having
reproduced or independently verified the full papers. Thresholds in the code are
versioned research assumptions, not transferred historical probabilities.

| Material | Principles retained | Application |
|---|---|---|
| **MarketDelta Strategy Guide**, MarketDelta LLC, 2007 | Profile identifies WHERE; footprint helps identify WHEN. Approximately 70% contiguous value area, PoC, VAH/VAL, high/low volume locations. Interpret signed aggression together with price response and location; breakouts need participation. | Executed-volume profiles, auction context, location quality, delta response and family-specific confirmation. No copied fixed crypto thresholds from the historical guide. |
| **Market Profile Study Guide**, Chicago Board of Trade | Repeated activity indicates acceptance; high-volume regions indicate balance, low-volume extremes can indicate rejection. Responsive activity returns toward value; initiative activity seeks new value. Range extension can accept or reject. | BALANCE, DISCOVERY_UP/DOWN, REJECTION_HIGH/LOW, TRANSITION and UNCLEAR; PoC shift and value overlap. Crypto uses measurable 24/7 sessions rather than pit-session assumptions. |
| **Crypto Range Trading material** (author/title not supplied) | Ranges can follow impulses. Meaningful boundaries, location, deviations, failed acceptance, re-entry and acceptance outside matter. Opposite-boundary travel is a hypothesis. | Closed-bar range width/ATR, compression, interactions, location and deviation features. Mid-range reversals receive weaker location evidence. |
| **Trading and Exchanges: Market Microstructure for Practitioners**, Larry Harris, 2003 | Spread includes transaction costs and adverse selection. Resting liquidity is vulnerable; passive fills are not automatically better. Crossing buys immediacy at a cost. | Spread, visible sweep impact, conservative slippage/funding/fee reserves and incomplete-fill handling. No claims of guaranteed stops or fills. |
| **To Cross or Not to Cross the Spread: That Is the Question**, Besson, Pelin & Lasnier, Journal of Trading, 2016 | Normalized imbalance is (bid size − ask size)/(bid size + ask size). Its relationship with trade side and passive execution needs venue-specific testing. | Touch-size OBI, 5/10/25-bps notional OBI, sampled persistence, microprice and depth concentration. Equity probabilities are not used as crypto probabilities. |
| **The Anatomy of Trading Algorithms**, Beason & Wahal | Aggressiveness changes fill probability, time, impact and adverse selection; order size relative to depth matters; unfilled orders contain information. | Visible depth consumed and market sweep estimates; participation-limited hypothetical fills. Passive probability/time-to-fill remain explicitly uncalibrated when observations cannot estimate them. |
| **Empirical Market Microstructure**, Joel Hasbrouck | Signed trades, response to flow, liquidity dynamics and causal quote/trade sequencing matter. | Executed trades and visible quotes retain different provenance. Decision features reject future availability timestamps. |
| **Automated Trading material**, Max Dama | Validate raw ticks, missing values, crossed books and gaps; use causal online features, rolling validation and simple baselines to control overfit. | Recorder hashes, continuity checks, rolling chronological data windows, logistic baseline, explicit missingness and abstention. |
| **A Machine Learning Based Pairs Trading Investment Strategy**, Sarmento & Horta | Preserve chronology across training, validation and testing; use forward windows rather than shuffled time-series folds. | Chronological train/validation/calibration/holdout partitions, outcome-aware purging and embargo; no future labels in decision inputs. |
| **High-Frequency Trading Patterns Around Short-Term Volatility Spikes**, Griffith, Van Ness & Van Ness | Microstructure relationships can change during volatility shocks. | Measured volatility regime; shock abstention and elevated execution uncertainty, rather than treating ordinary resting-book signals as universally reliable. |

## Insilico / Viscosity Solutions

[Market comment: Volume moves price](https://medium.com/@Insilico/2021-09-20-market-comment-volume-moves-price-unpublished-left-in-draft-then-published-09-23-775fda5bb455)
motivates testing participation at important breaks and the durability of V-shaped
reversals. A break without participation is weaker evidence; a V-bottom is not a
guaranteed durable bottom. These are hypotheses rather than laws.

[Art of speculation: expected value](https://medium.com/@Insilico/basic-series-art-of-speculation-trades-expected-value-finding-best-ev-offer-in-crypto-8c9839ea516d)
motivates evaluating net expected value rather than win rate alone, and separating
alt-specific residual return from BTC/ETH-driven beta. Closed, aligned log returns
estimate each factor relationship. The leader/vehicle idea is research-only: the
engine does not automatically choose the highest-beta alt or substitute a BTC
signal for an alt's own price-action and order-flow confirmation.

## Interpretation and validation

One visible quality score retains the original categories: regime, structure,
order flow, derivatives, execution, sourced fundamentals and cross-market context.
Horizon-aware evidence refines those categories without replacing the score with
model probability. Missing measurements are not zero-risk observations.

Primary paper outcomes, operational signal history and late directional evidence
are separate records. An expired setup that later reaches a target remains
expired. An original stop reached before the late target remains a loss under
the extended same-rules policy, even if directional afterlife is favorable.
Same-candle stop/target ambiguity is resolved conservatively; coverage gaps never
establish a delayed winner. None of these observations verifies a user's account
entry, position or realized return.
