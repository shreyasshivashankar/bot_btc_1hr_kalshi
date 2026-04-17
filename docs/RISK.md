# bot_btc_1hr_kalshi — Risk Policy

**Status:** Policy v1.0
**Changes require:** Explicit sign-off (risk owner). Logged in change log below.

This document is the **authoritative source** for risk limits. Code must reference these values by importing them from `src/bot_btc_1hr_kalshi/risk/policy.py`, which is generated from this document. If the two diverge, the document wins and the code is wrong.

---

## 1. Capital & Margin

- **Daily margin** is set at session open from the prior day's ending equity (not peak equity). Stored in `portfolio.daily_margin`.
- **Maximum deployable per session:** 25% of daily margin gross across all open positions.
- Margin is never rehypothecated intra-day — a loss reduces available margin immediately.

## 2. Per-trade sizing

- **Base allocation:** 2% of daily margin.
- **Scaling range:** 1% (chaotic vol) to 4% (compressed vol).
- **Kelly cap:** actual allocation ≤ ½ × empirical Kelly fraction for the active trap.
- **Hard per-trade ceiling:** 5% of daily margin. No sizing combination may breach this.

## 3. Loss limits (enforced top-down)

| Limit                            | Threshold                 | Response                                        |
| -------------------------------- | ------------------------- | ----------------------------------------------- |
| Single trade loss                | > 15% of daily margin     | Drawdown Freeze — 60 min API lockout            |
| Consecutive losses               | 3 in 30 min               | 15 min pause                                    |
| Daily realized loss (soft)       | > 5% of daily margin      | Sizing halved for rest of session               |
| Daily realized loss (hard)       | > 10% of daily margin     | No new entries; close existing at plan targets  |
| Weekly realized loss             | > 20% of weekly starting  | Full halt; mandatory review before next session |
| Monthly realized loss            | > 30% of monthly starting | Strategy decommission review                    |

Strategy-level **hit rate floor:** rolling 30-day hit rate ≥ 48%. Below this, mandatory review.

Strategy-level **Sharpe floor:** rolling 30-day paper Sharpe ≥ 0.8. Below this, mandatory review.

## 3.1 Soft stop-loss (intra-trade, relaxed)

The 15% single-trade hard freeze (above) remains inviolable. Below that, we apply a **soft stop** — a normal exit path that closes the position *before* the hard freeze can trip. This is deliberately not a fixed percentage; it adapts to regime and time of hour.

```
soft_stop_cents = entry_price_cents × (1 − SOFT_STOP_FRACTION)
SOFT_STOP_FRACTION = base_fraction × regime_multiplier × time_multiplier
```

| Parameter               | Default | Range (tunable in `config/params.yaml`) |
| ----------------------- | ------- | --------------------------------------- |
| `base_fraction`         | 0.35    | 0.25 – 0.50                             |
| `regime_multiplier`     | 1.0     | 0.7 (compressed vol) – 1.3 (chaotic)    |
| `time_multiplier`       | 1.0 → 0.5 | Linear from XX:00 to XX:45            |

Trigger: Kalshi mid ≤ `soft_stop_cents` for ≥ 2 consecutive seconds (debounce). Action: cancel resting theta-net, fire IOC at `best_bid − 3¢`. This is a normal exit — **no lockout, no halt**, bot continues next hour.

The soft stop is a **tunable parameter**, not a circuit breaker. Changes go through the weekly parameter tuning loop (§RUNBOOK Tuning procedure), not the risk committee.

## 3.2 Profit-taking early cash-out (99% probability)

When Kalshi prices an open YES position at ≥ 99¢ (or a NO position at ≤ 1¢), the position is closed immediately via IOC at `best_bid − 1¢`.

**Rationale:** the remaining 1¢ of upside does not justify the capital lock-up cost through settlement. Freeing capital to recycle into the next hour's market is the single largest contributor to realized Sharpe in active sessions.

| Parameter                                 | Default | Range        |
| ----------------------------------------- | ------- | ------------ |
| `BOT_BTC_1HR_KALSHI_EARLY_CASHOUT_THRESHOLD_CENTS`     | 99      | 95 – 99 only |

Raising above 99 is not permitted (liquidity at 100¢ is non-existent). Lowering below 95 requires risk sign-off — it trades guaranteed EV for capital turnover at an increasingly unfavorable rate.

## 3.3 Abandoned-to-Settlement state

When end-of-hour IOC escalation fails to find any bid at all (§DESIGN 7.3.3), the position enters a formal `ABANDONED_TO_SETTLEMENT` state rather than being orphaned.

- **Capital treatment:** margin allocated to the position remains locked until Kalshi resolution. It is **excluded from the next session's deployable margin** calculation. This prevents over-allocation in the hour immediately following an abandonment.
- **Accounting:** the position is marked-to-market at `0¢` pessimistically (full-loss assumption) in the daily PnL until resolution. If it resolves a winner, the gain flows through as a late-settled profit on the resolution date.
- **Limit:** no more than **1 concurrent abandoned position**. A second abandonment while one is outstanding triggers a halt for end-of-session review — abandonment chains imply a liquidity environment the bot should not be trading in.

## 4. Exposure caps

- **Max concurrent open positions:** 3
- **Max correlated positions** (same hour, same direction): 1
- **Max open orders:** 10
- **Max child orders per parent:** 5
- **Single-hour gross exposure cap:** 25% of daily margin

## 5. Operational breakers

| Breaker                     | Trip condition                        | Action                                      |
| --------------------------- | ------------------------------------- | ------------------------------------------- |
| Clock drift                 | NTP offset > 250ms                    | Halt; operator fix required                 |
| Primary feed staleness      | > 2s                                  | Fail-over; if all feeds stale → halt+flatten |
| Reconciliation mismatch     | Local vs broker > 1 contract          | Halt; page; manual RCA                      |
| Order ack timeout           | No ack in 3s                          | Cancel-request; if not acked, treat as hung |
| Daily order submission cap  | > 500 orders / day                    | Halt; likely bug                            |

## 6. Tier 1 Macro Override

Two deterministic triggers. **NLP on unstructured news is explicitly not a trigger** — spoofed/misparsed headlines could force liquidation at severe slippage.

### 6.1 Scheduled calendar events (pre-emptive)

Source: structured economic calendar (ForexFactory / TradingEconomics JSON). Events with `impact=High`:
- Central bank rate decisions
- CPI / Core CPI releases
- NFP (Non-Farm Payrolls)
- PCE / Core PCE
- FOMC minutes

**Response:** Auto-flatten the book at **T−60 seconds** before the scheduled release. No new entries until T+30 minutes after release. This is *pre-emptive* — we are out of the market when the print hits, not reacting to volatility after the fact.

### 6.2 Human kill-switch (unscheduled)

For unscheduled qualitative events (exchange hacks, geopolitical shocks, unexpected regulatory announcements), the operator invokes `POST /admin/tier1_override` on the Cloud Run admin endpoint.

**Response:** Flatten entire book via IOC escalation ladder (§DESIGN 7.3.2). **No PnL conditionality** — winners and losers both close. 2h stabilization window before new entries.

### 6.3 Invariants

- No automated NLP-based flatten. Ever.
- Both scheduled and human-triggered flattens log to `bot_btc_1hr_kalshi.admin_audit` with the trigger source and operator identity.
- Any flatten invocation is a reviewable event — monthly policy review checks for unnecessary invocations.

## 7. Governance

- Parameter changes in `policy.py` require a PR labeled `risk-change` with named approver.
- Circuit breaker logic may not be bypassed, even temporarily. "Turn it off to debug" is not an acceptable pattern.
- Any operator-initiated halt override requires a same-day written RCA committed to `docs/incidents/`.

## 8. Change log

| Date       | Version | Change                     | Approver |
| ---------- | ------- | -------------------------- | -------- |
| 2026-04-16 | 1.0     | Initial policy             | Shrey    |
| 2026-04-16 | 1.1     | Added §3.1 soft stop (adaptive, tunable), §3.2 early cash-out (≥99¢ → IOC), §3.3 Abandoned-to-Settlement formal state. §6 Tier 1 rewritten: scheduled calendar pre-emptive flatten + human kill-switch; NLP explicitly forbidden. | Shrey |
