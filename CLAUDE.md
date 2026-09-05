# CLAUDE.md — June CFD Bot Operating Manual
> Authoritative reference for any Claude session working on this codebase.
> Verified against actual code at commit 9398f89 (2026-09-05).
> Previous CLAUDE.md: none (this is the first version).

---

## What June Is

**June** (`june.py`) is an IG CFD live trading bot. It scores momentum on a 60-second cycle, sizes positions from a `$10` floor, places orders on the IG live account, and manages exits with stop/TP/pyramid logic.

- **Version:** beta v0.2 (signal publisher) / v0.3 (live trading section)
- **VPS:** `root@162.243.53.236`, Ubuntu 24.04, `/opt/bots/june/`
- **Systemd service:** `june`
- **Git remote:** `https://github.com/jevon9325-cmd/june.git`, branch `main`
- **Python venv:** `/opt/bots/june/venv/`
- **Env file:** `/opt/bots/june.env`

June is separate from Miss Secretary and Claudia. It has its own Redis state, its own phase/sizing track, and its own performance history. The two systems can share Redis keys as a read-only signal channel (integration roadmap below) but never share position state.

---

## Architecture: Dual-Guard Lightstreamer + REST

June uses **two independent systems** to verify position state — LS (Lightstreamer) and REST — before any close order is placed or cleared.

### Lightstreamer (LS)
- `_ls_init_session()` subscribes `ACCOUNT` and `TRADE` channels on the IG Lightstreamer endpoint at startup.
- `_ls_confirms_closed` (set, line ~377): deal_ids confirmed **FULLY_CLOSED** via the TRADE stream `CONFIRMS` event. Thread-safe via `_ls_confirms_lock`.
- `_ls_account` (dict, line ~374): live `PNL`, `DEPOSIT`, `AVAILABLE_CASH`, `MARGIN`, `EQUITY` — updated in-thread on every ACCOUNT tick.
- `_ls_deal_closed(deal_id)` (line ~7744): lock-protected lookup against `_ls_confirms_closed`.
- `_ls_get_margin()` (line ~7744): returns current margin from LS ACCOUNT, or None if LS not connected.

### REST
- `_get_preferred_deposit()`: calls `/accounts` to get the current deposit (margin in use). Returns ≥0.0.
- `/positions/otc` returns **404 on this IG account** — this is a confirmed permanent limitation. All position checks use `/accounts` deposit as the REST proxy for "position still open".

### Dual-Guard Logic (`_post_dual_verify()` — line ~8015)
After every position close order, `_post_dual_verify()` runs up to `_VERIFY_MAX` (3) times:
- `_ls_flat = _ls_ok and (_ls_m == 0.0 or _ls_cl)` — LS says flat
- `_rs_flat = _rs_ok and _dep == 0.0` — REST deposit is zero
- Both flat → `return True` (open_position cleared, cycle continues)
- Both open → `return None` (retry)
- Disagreement → **Pyramid Promotion check** (see below), else `manual_review_required=True`

### Pre-Send Guard (`_ls_position_guard_check()` — line ~7751)
Before sending a close order, compares `open_position.deal_id` against `_ls_confirms_closed` and REST deposit. On disagreement, sets `manual_review_required` and aborts. Returns `(open_bool, source_str)`.

### Pyramid Promotion Logic (line ~8045, commit `9398f89`)
Inside the `_post_dual_verify()` disagreement branch:
- **Condition:** `_ls_cl=True` (primary CONFIRMS received) AND `len(pyramid_legs)==1` AND addon deal NOT in `_ls_confirms_closed` AND `_dep > 0`
- **Action:** Promote the addon leg to `open_position`, clear `pyramid_legs` and `pyramid_agg_stop_level`, log `[PYRAMID PROMOTE]`, save state, return `False`
- **Multi-addon fallback:** `len(pyramid_legs)>1` falls through to `manual_review_required` — cannot safely guess which leg to promote

### `manual_review_required`
Redis-persisted flag in `june_live_state`. When set:
- All close operations for the affected symbol are blocked
- Clears only when `_live_reconcile_positions()` confirms flat (deposit=0 AND LS margin=0)
- Root cause: a state where LS and REST give conflicting signals that aren't resolvable automatically

---

## Instrument List (Current)

### FX Pairs — STRUCTURALLY EXCLUDED
All FX pairs are in `INSTRUMENTS` and scored for signals, but **structurally blocked from live trading** (`_live_fx_instruments` set populated at startup from epic pattern `CS.D.*.CFD.IP`). Reason: cheapest pair (AUDUSD) needs ~$4,800 minimum notional; current balance ~$24 (215× below). Guard will persist until balance reaches a viable threshold. FX instruments still participate in scoring and SIM.

| Symbol | Epic | Status |
|--------|------|--------|
| EURUSD | CS.D.EURUSD.CFD.IP | Scored; live-blocked (balance) |
| GBPUSD | CS.D.GBPUSD.CFD.IP | Scored; live-blocked (balance) |
| USDJPY | CS.D.USDJPY.CFD.IP | Scored; live-blocked (balance) |
| AUDUSD | CS.D.AUDUSD.CFD.IP | Scored; live-blocked (balance) |
| USDCAD | CS.D.USDCAD.CFD.IP | Scored; live-blocked (balance) |
| EURGBP | CS.D.EURGBP.CFD.IP | Scored; live-blocked (balance) |
| NZDUSD | CS.D.NZDUSD.CFD.IP | Scored; live-blocked (balance) |
| USDCHF | CS.D.USDCHF.CFD.IP | Scored; live-blocked (balance) |

### Commodities — Live eligible at ~$22 balance
All use the concentration cap (`_sim_is_eligible()`): `min_notional / leverage ≤ 20% of balance`.

| Symbol | Epic | Asset class | Margin | Price unit | Min stop |
|--------|------|-------------|--------|------------|---------|
| GOLD | CS.D.CFDGOLD.BMU.IP ¹ | METAL | 2% | 1.0 | 4pts |
| SILVER | CS.D.CFDSILVER.BMU.IP ¹ | METAL | 10% | 0.01 | 12pts |
| OIL (Brent) | CC.D.LCO.BMU.IP ¹ | ENERGY | 10% | 0.01 | 12pts |
| NATGAS | CC.D.NG.BMU.IP ¹ | ENERGY | 3% | 1.0 | 60pts |
| WHEAT (Chicago) | CC.D.W.BMU.IP ¹ | METAL | 10% | 0.01 | 5pts |
| COCOA (London) | CC.D.LCC.BMU.IP ¹ | METAL | 10% | 1.0 | 20pts |
| LWB (London Wheat) | CO.D.LWB.FBMU3.IP ¹ | METAL | 10% | 1.0 | 2pts |
| SUGAR (London No.5) | CC.D.LSU.BMU.IP ¹ | METAL | 10% | 1.0 | 3pts |
| HO (Heating Oil) | CC.D.HO.BMU.IP ¹ | ENERGY | 10% | 0.01 | 100pts |

¹ These epics fail the startup `/markets/{epic}` check when IG rate-limits (403); the epic-discovery fallback re-finds them via `/markets?searchTerm=...` and updates `INSTRUMENTS` dict in-process for the session.

### Indices — Live eligible
| Symbol | Epic | Margin |
|--------|------|--------|
| SPX500 | IX.D.SPTRD.IFM.IP | 2% |
| GER40 | IX.D.DAX.IFD.IP | 2% |
| UK100 | IX.D.FTSE.CFD.IP | 2% |

### Crypto CFDs — Live eligible, balance-gated
| Symbol | Epic | Margin | Min notional | Min balance (approx) |
|--------|------|--------|-------------|----------------------|
| BTC | CS.D.BITCOIN.CFD.IP | 10% | ~$807 (live) | ~$1,345 (20% cap, 10% margin, lev=3) |
| ETH | CS.D.ETHUSD.CFD.IP | 10% | ~$100 (live) | ~$167 |

**BTC/ETH minStop:** IG returns `minNormalStopOrLimitDistance.unit=PERCENTAGE` for these. The code detects this, logs a warning, and falls back to **4pt** — which is far too tight for BTC (~162pts real minimum). BTC orders placed with 4pt stops will be rejected by IG. **Fix pending (backlog).** In practice BTC/ETH don't trade until balance is large enough for the concentration cap anyway.

BTC and ETH bypass the weekend gate via `_CONTINUOUS_INSTRUMENTS` (24/7 markets).

### US Equity CFDs — Live eligible, priced via Finnhub REST
Prices are fetched from Finnhub REST (not IG streaming — `streamingPricesAvailable=False` on these epics). All verified on live IG account.

Active in `INSTRUMENTS`: NVDA, TSLA, AAPL, MSFT, AMD, INTC, MU, SPCX (SpaceX, listed 2026-06-12).  
Mapped in `direct_cfd_map` (Redis key `june_direct_cfd_map`): 40 symbols including AMZN, GOOGL, META, AVGO, BA, RTX, LMT, XOM, CVX, and others found via IG search.

**Equity leverage gate:** If `minDeal` clamp produces an effective leverage exceeding the phase ceiling by >0.5×, the trade is blocked with `EQUITY LEV GATE` log.

---

## Scoring and Entry

### Signal Generation
Each cycle: 60-second rolling price history (`_history` deque, 20 readings). Signals computed per instrument:
- `vol` = `|change_5m|` — 5-minute momentum
- `direction` = `bull` or `bear` based on sign + magnitude
- `conviction` = integer 1–10, assembled from streak, persistence, slow-grind, Barbie override, exhaustion penalty
- `spread_atr_ratio` (SAR) = current spread / 14-period ATR from same history
- `spread_atr_wide` = True when SAR > hybrid tier threshold (FX 0.35, METAL 0.60, ENERGY 0.85, CRYPTO 1.50)

### Conviction Points Sources
- **Streak pts** — consecutive same-direction cycles
- **Persistence confirmed** — direction matched the prior cycle (`_last_cycle_direction`)
- **Slow-grind pts** — `_slow_grind_pts()`: ≥75% of 19 cycles in same direction, net move ≥ threshold (OIL 0.28%, SILVER 0.22%) → +1pt cap
- **Claudia directive** — `_load_claudia_directive_notes()` reads `session_directive` from Redis; sector alignment or symbol in `high_conviction` → soft boost
- **Barbie overrides** (`_claudia_directive_notes`) — USDJPY, OIL, SILVER, COCOA, NATGAS flagged as active overrides
- **Exhaustion reduction** — `_exhaustion_ratio() ≥ 2.5×` ATR → conviction -1; `≥ 3.5×` → entry blocked (`_EXHAUST_RATIO_BLOCK`)

### Spread/ATR Entry Gate
Hybrid tiered thresholds per asset class (see above). When `spread_atr_wide=True`:
- Position size scaled: `pos_size × (threshold/SAR)^0.5`
- Score penalised in ranking

### 15m Reliability Gate (`_sim_15m_gate_mode()`)
Per-instrument-direction reliability score from a rolling 20-trade window. Modes: RELAXED (cold), MODERATE (deadzone ±0.05%), STRICT (must align). Decision-affecting in SIM; also used in live via the same function.

---

## Live Sizing and Leverage

### Position Size Formula
```
# Flat $10 below $100 balance; 10% of balance above. No jump at crossover.
pos_size = max(2.0, min(10.0, bal) if bal < 100.0 else round(bal * 0.10, 2))
```
- **$2 floor** — hard minimum
- **Spread scale** — when `spread_atr_wide`, multiply by `(threshold/SAR)^0.5` (clamped ≥ $2)
- **Macro scale** — Claudia alignment: 1.0 aligned, 0.8 neutral/stale, 0.5 conflict
- **Observer moderate** — multiply by 0.70 (see perf-block below)
- **notional = pos_size × leverage**

### Leverage Phase System (Live)
Separate from SIM. Phase ceiling applied when balance ≥ `_LIVE_PHASE_GATE_BAL` ($200). Below $200 (current operating range), leverage passes through conviction-based calculation unchanged.

| Phase | Max lev | Advance criteria | Drop-back |
|-------|---------|-----------------|-----------|
| 1 (current) | 3× | 10 trades, ≥50% WR, PnL>0 | — |
| 2 | 5× | 20 trades, ≥55% WR, +5% balance | 5 consec losses or -5% balance |
| 3 | 10× | — | 5 consec losses or -5% balance |

Conviction-based leverage: `_sim_conviction_leverage("sprout", conv)` maps conviction 1–10 to leverage within the phase ceiling.

### Leverage-Scaled Stop Tightening
Active only when balance ≥ $200 AND leverage > 3 (Phase 1 floor):
```python
_lev_mult = (3 / leverage) ** 0.5   # sqrt dampening
stop_pct  = max(stop_pct * _lev_mult, _spread_floor)
```
Not empirically validated — reasoned starting point, revisit when Phase 2+ data exists.

### Counter-Trend SL Compression
When Claudia macro is `conflict` (0.5× size), `stop_mult=0.8` is passed → `stop_pct × 0.8`. Belt-and-suspenders: tighter SL + smaller size.

---

## Pyramid System

### Structure
- 1 primary position + up to N addon legs tracked in `_live["pyramid_legs"]` (list of leg dicts)
- Current hard cap: 2 legs (1 primary + 1 addon). Counter-unlock via Redis key `june_pyramid_2leg_completions`
- Leg 3 unlocks after 10 confirmed 2-leg completions; leg 4 after 20
- Leg size decay: leg 3 = leg2_notional × 0.75; leg 4 = leg2_notional × 0.50

### Aggregate Stop
`pyramid_agg_stop_level` = 0.5% from blended entry price across all legs. When breached, all legs close simultaneously.

### Pyramid Leg Dict Schema
```python
{
    "instrument": sym, "direction": dirn, "deal_id": deal_id, "deal_ref": deal_ref,
    "fill_price": fill_price, "ig_size": ig_size, "notional": notional,
    "stop_pct": stop_pct, "tp_pct": tp_pct, "entry_time": time.time(), "leg_index": leg_index
}
```

### Promotion Logic (commit `9398f89`)
When the primary closes and an addon remains open, `_post_dual_verify()` promotes the addon to `open_position`. The promoted dict carries `_pyramid_promoted=True` as audit flag. `pyramid_legs=[]`, `pyramid_agg_stop_level=None` after promotion.

### Orphan Protection
`if not open_position and pyramid_legs:` → `_live_close_all_addon_legs("orphan_primary_closed")`. Fires in the main cycle check, not during `_live_close_position()`.

---

## Safety Mechanisms

### Kill Switch
- Redis key `june_live_enabled` (string). Default OFF.
- `_refresh_live_kill_switch()` runs every cycle; updates `_june_live_trading_enabled`.
- Enable: `redis-cli SET june_live_enabled true` — Disable: `redis-cli SET june_live_enabled false`
- When OFF: scoring, SIM, logging, exit management all run; only NEW position placement is blocked. Exits always execute.

### Circuit Breaker (`_live_check_circuit_breaker()` — line ~6555)
Tiered by balance:

**Micro-account tier** (balance < $30):
- Buffer = `max($2.00, 30% × day_start_balance)`
- Fires after ~3rd stop-out at current position sizes (~$0.83/stop-out)

**Full-account tier** (balance ≥ $30):
- Buffer = `max(min($20, 10% × day_start), 5% × day_start)`
- Effective threshold: 10% until balance > $400, then 5% (once 5%×day > $20 floor)
- At $24 balance: buffer ≈ $2.40 (10% effective)

**Effect:** Sets `june_live_enabled=false` in Redis and logs `june_circuit_breaker_alert` (48h TTL). Recovery requires manual `SET june_live_enabled true` after investigating.

### Defensive Mode (`_live_update_defensive_mode()` — line ~6202)
Middle layer: fires at **half** the CB buffer. Reduces sizing but does not halt trading.

### Performance Block System (`_live_perf_record()` — line ~7085)
Per-instrument rolling stats stored in Redis key `june_perf_stats:{sym}`.

**WR-based severity tiers** (rolling 14-day window, min 8 recent trades):
| Tier | WR | Net loss | Effect |
|------|----|----------|--------|
| Observer Light | <30% | <3% balance | Conviction floor ×1.5 |
| Observer Moderate | <30% | ≥3% balance | Conviction floor ×2, size ×0.70 |
| Hard Block (12h) | <20% | >7% balance, ≥12 trades | All new positions blocked 12h |

**SAR-based block** (session-scoped):
- Fires when avg Spread/ATR > 50% across ≥4 **valid-SAR** trades in the current sub-session
- Uses `entry_sar` (not `sar`) when available; skips zero-value sentinel records
- TTL = time to next sub-session boundary (overnight / pre-NYSE / NYSE-morning / NYSE-afternoon)
- Redis key: `june_perf_block_sar:{sym}:{sub_session}`

**Defect quarantine registry** (`_DEFECT_QUARANTINE`): manually maintained list of epoch windows where a diagnosed+fixed code defect produced bad data. Quarantined trades are excluded from WR/SAR calculations with `excluded_defect_id` tag.

### Exhaustion Gate (`_exhaustion_ratio()` — line ~3799)
Ratio = net directional move over `_history` window / 5m ATR. Detects "chasing exhausted moves":
- Ratio ≥ 2.5 → conviction -1
- Ratio ≥ 3.5 → entry blocked entirely
- Calibrated on OIL/SILVER data; only active for instruments in `_GRIND_THRESH`.

### Quarantine Mechanism
Same `_DEFECT_QUARANTINE` registry. No auto-population — MANUALLY add only when a structural fix is committed in the same patch. Tagging without a corresponding code fix is not permitted.

### Manual Review Required (`manual_review_required`)
Redis-persisted in `june_live_state`. When set: all close operations blocked. Cleared only by `_live_reconcile_positions()` confirming truly flat (LS deposit=0 AND REST deposit=0). Set by:
- `_ls_position_guard_check()` on pre-send disagreement
- `_post_dual_verify()` on post-close disagreement that doesn't qualify for pyramid promotion

### Reconciliation (`_live_reconcile_positions()` — line ~9561)
Called at startup and after failed closes. Sequence:
1. Try `/positions/otc` (always 404 on this account — expected)
2. Fall back to `/accounts` deposit check
3. If deposit > 0 → skip (preserve state as "transitional")
4. If deposit = 0 AND LS margin = 0 → confirm flat, clear `open_position`, clear `manual_review_required`

---

## Observation-Only Layers

These systems log and accumulate data but do **not** currently affect entry/exit decisions. A future session must not mistake them for decision-affecting mechanisms without first verifying the code.

### HTF Alignment Pipeline (`_HTF_INSTRUMENTS`, line ~5992)
- Fetches hourly candles from IG REST for GOLD, OIL, SILVER, WHEAT, NATGAS
- Computes 3-hour directional bias: `bull` / `bear` / `neutral` (noise floor 0.3%, saturation 0.8% — provisional)
- Logs `[HTF DIAG]` at each entry candidate evaluation
- Records `htf_bias` in `open_position` and `_live_write_htf_event()` on close
- Self-calibration: `_htf_self_calibrate()` cross-matches HTF events against trade history; logs `[HTF CALIB]` with aligned/opposed WR split
- Readiness alert (`[HTF READY FOR REVIEW]`) fires once when: ≥30 events, ≥10pp aligned vs opposed WR gap, at least 10 new events since last calibration
- Redis key: `june_htf_events` (500-entry cap, 30-day TTL)
- **Status: observation-only.** Wire it into the entry gate only after WR split is confirmed significant (10pp gap, ≥30 events).

### Exhaustion/SAR Timeseries (`june_live_ex_ratio_log`)
- `_live_write_ex_ratio_obs()` logs per-cycle `ex_ratio` + `spread_atr_ratio` for OIL, SILVER, NATGAS
- 2000-entry cap, 7-day TTL
- **Status: observation-only.** For calibrating exhaustion thresholds.

### SIM→Live WR Correlation
- `open_position` carries `_pyramid_promoted: True` (audit) and `persistence_confirmed` (one-prior-cycle direction agreement)
- `_live_perf_record()` stores `persistence_confirmed` and `entry_sar` per trade for future analysis
- **Status: observation-only.** The SIM win-rate is tracked in `june_sim_state` separately from live.

### `persistence_confirmed` Field
Stored in `open_position` and in perf log. True when `_last_cycle_direction[sym]` matched the entry direction on the prior cycle. **Not used in any decision gate** — purely for post-hoc WR analysis by persistence vs non-persistence entries.

---

## SIM Layer (Parallel Simulation)

Runs every cycle alongside live. Separate Redis state (`june_sim_state`). Stage progression: `sprout → seedling → germination → vegetative → full_bloom`. Each stage has minimum trades, WR, and P&L-pct requirements. On graduation, balance is injected to a stage floor (e.g. seedling → $70, germination → $300).

SIM has its own phase/leverage track (`_sim_check_phase()`), separate from the live phase. SIM conviction-leverage mapping: `_sim_conviction_leverage("sprout", conv)` → leverage 3–10 within stage range.

---

## Macro Confluence (`_live_macro_confluence()` — line ~7323)

Reads `_claudia_directive_notes` (Claudia's `session_directive` Redis key) and freshness from `claudia_sector_momentum` (stale if >15 min old). Returns:

| Result | Scale | SL compress |
|--------|-------|-------------|
| Claudia aligned with local signal | 1.0× | No |
| Claudia neutral / stale / missing | 0.8× | No |
| Claudia conflicts with local signal | 0.5× | Yes (0.8× stop_mult) |

Alignment is via `_JUNE_SECTOR_MAP` (instrument → sectors) matched against Claudia's `thesis_sectors` / `avoid` / `high_conviction` lists.

---

## Profit Skim

`_live_check_skim()` — currently **disabled** (`_SKIM_ENABLED = False`). Logic is coded and tested but intentionally switched off pending the $443 sizing crossover (balance where 10% = $44.30 → meaningful skim increment). Re-enable at ~$443 by setting `_SKIM_ENABLED = True`.

Phase pre-$300: no skim. Phase $300+: $100 increments flagged as `skim_pending`. Phase $500+: half-mode (hourly minimum gap). Tradeable capital = `balance_total - skimmed_total`.

---

## Redis Keys (June-Specific)

| Key | Purpose | TTL |
|-----|---------|-----|
| `june_live_state` | `_live` dict: open_position, pyramid_legs, balance, phase, perf state | No expiry |
| `june_sim_state` | SIM `_sim` dict | No expiry |
| `june_live_enabled` | Kill switch (string "true"/"false") | No expiry |
| `june_signals` | Per-instrument momentum signals | 120s |
| `june_morning_baseline` | Overnight price summary | 2h |
| `june_premarket_gaps` | Pre-London gaps >0.5% | 3h |
| `june_spread_baselines` | Rolling 1h spread averages | 25h |
| `june_overnight_context` | Volatility regime + correlation notes | 4h |
| `june_perf_stats:{sym}` | Per-instrument rolling trade stats | No expiry |
| `june_perf_block_sar:{sym}:{session}` | SAR block active flag | Session-scoped TTL |
| `june_pyramid_2leg_completions` | Counter for pyramid leg unlocks | No expiry |
| `june_live_ex_ratio_log` | Exhaustion/SAR timeseries | 7 days |
| `june_htf_events` | HTF event log for calibration | 30 days |
| `june_circuit_breaker_alert` | CB alert payload | 48h |
| `june_min_notionals` | Min notional values (survives sim resets) | 7 days |
| `june_direct_cfd_map` | IG epic lookup for equity CFDs | 24h |
| `june_price_history` | Rolling price deques per instrument | 60 min |

---

## VPS Paths and Commands

```bash
# Service management
systemctl restart june
systemctl status june
journalctl -u june -n 100 --no-pager
journalctl -u june -f

# Git
cd /opt/bots/june && git log --oneline -5
cd /opt/bots/june && git pull origin main && systemctl restart june

# Syntax check
python3 -m py_compile /opt/bots/june/june.py && echo OK

# Kill switch
redis-cli SET june_live_enabled true   # enable
redis-cli SET june_live_enabled false  # disable (default)

# State inspection
redis-cli GET june_live_state | python3 -c "import json,sys; s=json.load(sys.stdin); p=s.get('open_position') or {}; print(p.get('deal_id'), p.get('fill_price'), s.get('manual_review_required'))"
```

---

## Engineering Policies

### Verification Standard (Non-Negotiable)
Before any claim of "done" on a code change:
1. **diff** — review the actual patch (not just the description)
2. **py_compile** — `python3 -m py_compile /opt/bots/june/june.py && echo OK`
3. **Trace** — walk through the specific scenario being fixed step-by-step with real values from the incident, confirming the new path fires and all other paths are unaffected

Never substitute memory of what the code "should" do for an actual trace.

### Restart Policy
- **Live position open:** Full pre-restart snapshot (REST + Redis), explicit confirmation of the same deal_id in the post-restart resume log, Redis state verification after restart. No shortcuts.
- **Confirmed flat and halted:** Relaxed — syntax check + brief log scan sufficient.
- Restart is always the final step, after a full report and explicit go-ahead from the user.

### Patching Policy
- Always take a backup before patching: `cp june.py june.py.bak_<description>`
- Patch scripts go to `/tmp/` (VPS) or session scratchpad (local)
- String anchors must be exact and unique; the patch script must assert this before writing
- After writing: diff + py_compile both must pass before considering the patch applied

### Evidence-Gated Thresholds
New thresholds (SAR caps, exhaustion ratios, HTF noise floors, etc.) must come from measured data, not intuition. The `[HTF READY FOR REVIEW]` alert is the formal gate for HTF calibration. Don't enable a mechanism based on calendar time — wait for the evidence gate to fire.

### Commit Style
- Prefix: `fix:` / `feat:` / `refactor:`
- Body: root cause, what changed, what was verified, what paths are unaffected
- Always end with `Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>`

---

## Known Backlog / Deferred Items

| Item | Status | Notes |
|------|--------|-------|
| **BTC/ETH minStop PERCENTAGE fix** | Pending | IG returns `unit=PERCENTAGE` for crypto min stop; code falls back to 4pt. Real BTC minimum ~162pts. Fix: when unit=PERCENTAGE, derive pts from `value% × mid / price_unit`. |
| **XRP/SOL spread/ATR measurement** | Pending | CRYPTO tier threshold (1.50) calibrated on BTC/ETH. XRP and SOL need 14:00-20:00 UTC measurement session before being added. |
| **Pyramid 3–4 leg extension** | Designed, gated | Code exists (leg 3 @ 10 completions, leg 4 @ 20). Unlock is automatic once the `june_pyramid_2leg_completions` counter is hit. No code change needed — just needs trading history to accumulate. |
| **HTF alignment wire-in** | Observation phase | `[HTF READY FOR REVIEW]` alert must fire (≥30 events, ≥10pp WR gap) before wiring HTF into the entry gate. Currently observation-only. |
| **Slow-grind detector** | Coded (OIL+SILVER only) | `_slow_grind_pts()` implemented. Only active for OIL and SILVER (in `_GRIND_THRESH`). Recalibrate thresholds when balance >$250. |
| **Skim re-enable** | Waiting for balance ~$443 | `_SKIM_ENABLED = False`. Flip to True when 10% sizing = meaningful skim increment. |
| **SAR min-sample fix** | **Done (commit 119e42b)** | SAR block now gates on valid-SAR count (entry_sar > 0), not total trade count. Live since 2026-09-05 restart. |
| **Miss Secretary integration** | Roadmap | `june_signals` → `score_candidate()` boost in Miss Secretary. Wire after 2+ weeks of signal quality validation. |
| **LLM P&L monitor** | Future | `OPENROUTER_API_KEY` and `GEMINI_API_KEY` present in env; no code yet. |

---

## Key Function Index

| Function | Location | Purpose |
|----------|----------|---------|
| `run_live_step()` | ~line 9520 | Top-level live cycle: exits then entry |
| `_live_try_entry()` | ~line 9230 | Entry evaluation, sizing, guard checks |
| `_live_open_position()` | ~line 7399 | Order placement (only place live orders are placed) |
| `_live_close_position()` | ~line 7802 | Close primary position with dual-verify |
| `_post_dual_verify()` | ~line 8015 | LS+REST post-close agreement check + pyramid promotion |
| `_ls_position_guard_check()` | ~line 7751 | Pre-send LS+REST guard |
| `_ls_init_session()` | ~line 7617 | Lightstreamer ACCOUNT+TRADE subscription setup |
| `_ls_deal_closed()` | ~line 7744 | Check deal_id against `_ls_confirms_closed` |
| `_live_reconcile_positions()` | ~line 9561 | Startup/post-failure reconciliation |
| `_live_check_circuit_breaker()` | ~line 6555 | Daily drawdown CB check |
| `_live_update_defensive_mode()` | ~line 6202 | Defensive mode trigger |
| `_live_perf_record()` | ~line 7085 | Update per-instrument rolling stats + fire blocks |
| `_live_phase_leverage()` | ~line 9159 | Phase → max leverage |
| `_live_check_phase()` | ~line 9166 | Phase advancement/drop-back |
| `_live_macro_confluence()` | ~line 7323 | Claudia alignment → size scale |
| `_pyramid_active_max_legs()` | ~line 8910 | Current max pyramid legs (reads Redis counter) |
| `_live_check_pyramid_entry()` | ~line 8870 | Pyramid addon entry gate |
| `_live_check_pyramid_exits()` | ~line 8777 | Per-leg SL/TP exit check |
| `_exhaustion_ratio()` | ~line 3799 | Net move / ATR exhaustion ratio |
| `_slow_grind_pts()` | ~line 3750 | Multi-cycle momentum conviction boost |
| `_compute_htf_alignment()` | ~line 6042 | HTF directional bias (observation-only) |
| `_htf_self_calibrate()` | ~line 6092 | HTF cross-match and WR calibration |
| `_live_write_ex_ratio_obs()` | ~line 5955 | Log ex_ratio timeseries (observation-only) |
| `_live_check_skim()` | ~line 6485 | Profit skim (currently disabled) |

---

## Owner Context

- Account: IG CFD account Z6CPCQ (demo), HT2Q8 (live intelligence — W-8BEN, no orders)
- Live orders go on Z6CPCQ
- Balance at last verified state (2026-09-05): ~$24.21, WHEAT SHORT DIAAAAR4ZHBAQAL @ 755.4 open
- All timestamps in Jamaica time (UTC-5, no DST = ET-1h always)
