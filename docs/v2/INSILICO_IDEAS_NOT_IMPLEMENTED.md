# Ideas excluded from the production upgrade

See the [research ledger](INSILICO_RESEARCH_LEDGER.md) for source locations and the [gap analysis](BYBITFLOW_V1_TO_V2_GAP_ANALYSIS.md) for existing equivalents.

- **Hidden participant identity and intent — REJECT.** Parent orders can split. Public print size cannot establish institutions, insider information, actual trapped account inventory or private motives.
- **Options gamma/GEX — DEFER.** Requires a causal options dataset and explicit dealer positioning assumptions. Perpetual funding and OI are not substitutes.
- **Social/crowd sentiment — DEFER.** Behavioral books give conceptual context; no reproducible production dataset is present. No expensive social ingestion is justified by this archive alone.
- **Risk-free breakouts and automatic destination targets — REJECT.** The video includes absolute claims; they lack validation. Existing stop, flow, liquidity and cost checks remain mandatory. A destination is a reference, not a guaranteed fill.
- **Fixed contract/dollar cutoffs and pit-session behavior — REJECT.** Futures contract units, equity tick sizes and US openings do not transfer literally to continuously traded crypto. Use original-venue, symbol-relative observations.
- **An 85% overlap probability — REJECT as a crypto prior.** A discretionary historical claim does not calibrate current crypto outcomes.
- **A new footprint/profile/beta engine — REUSE_EXISTING.** These calculations already exist. Reimplementation would add compute and increase inconsistency risk.
- **Multiple Renko/range/delta/imbalance chart engines — DEFER.** One causal bounded activity summary is sufficient for the initial upgrade. Additional histories need evidence of marginal value and latency testing.
- **Hull MA or an optimal MA setting — DEFER.** No inspected source establishes a portable, validated advantage over existing closed-bar trend/efficiency state.
- **Standalone pairs/RL bots, market-making and account execution — RESEARCH_ONLY.** These solve different portfolio/inventory problems and conflict with the public-data/manual setup scope.
- **Simulated fill probabilities and stochastic-model stop optimization — DEFER.** Without a validated model and real execution data, model assumptions are not observed fills or optimal risk parameters.
- **Mathematical absorption as order-flow absorption — REJECT.** Markov absorbing states and defended aggressor flow are different concepts.
- **Correlation as causation or cointegration — REJECT.** Reuse causal factor/residual analysis and preserve stability checks. Do not trade an altcoin solely for high beta.
- **Quantum computation and general game-theory equilibria as direct signals — NOT_USEFUL / RESEARCH_ONLY.** These provide no measurable production rule with the available data.
- **A v1/v2 shadow scanner — REJECT.** Historical cohorts remain available for research, but new opportunities use one v2 pipeline. Active old plans continue their original lifecycle independently.
- **Unverified linked YouTube lecture — DEFER.** The archive TXT was read; the linked lecture contents were not available to the reader. No implementation is attributed to its unseen contents.
