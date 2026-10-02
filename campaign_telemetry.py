"""Bounded experimental observations, independent of durable broker evidence.

No function returns a trading decision or mutates the supplied trading state.
Prices/economics are sampled estimates, never certified fills or broker net P&L.
"""
import hashlib
import json
import math
import sqlite3
from pathlib import Path
from winner_protection import effective


def number(value):
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (ValueError, TypeError):
        return None


def executable(signal, direction):
    """Prefer actual exit side; otherwise explicitly label reconstructed spread."""
    side = "bid" if direction == "long" else "offer"
    price = number(signal.get(side))
    if price is not None and price > 0:
        return price, side + "_observed"
    mid, spread = number(signal.get("price")), number(signal.get("spread_pct"))
    if mid is None or mid <= 0 or spread is None or spread < 0:
        return None, "missing_executable_price"
    price = mid * (1 - spread / 200 if direction == "long" else 1 + spread / 200)
    return (price, side + "_reconstructed_from_mid_spread") if price > 0 else (None, "invalid_price")


FIELDS = ("deal_id", "deal_ref", "instrument", "direction", "fill_price", "ig_size",
          "notional", "actual_notional", "intended_notional", "pos_size", "leverage",
          "entry_time", "original_notional", "partial_dollar_pnl", "partial_exit_done",
          "partial_exit_pending", "stop_pct", "tp_pct", "peak_pnl_pct", "dple_effective_sl",
          "intended_stop_level", "acknowledged_stop_level", "broker_stop_level",
          "defensive_stop_level", "defensive_soft_sl", "defensive_stop_active", "stop_sync", "leg_index")


class Store:
    def __init__(self, path, *, sample_cap=50000, event_cap=20000, campaign_cap=2000, retention=30*86400):
        self.path = str(path)
        self.sample_cap, self.event_cap = sample_cap, event_cap
        self.campaign_cap, self.retention = campaign_cap, retention

    def connect(self):
        db = sqlite3.connect(self.path, timeout=.05)
        try:
            self.initialize(db)
        except Exception:
            db.close()
            raise
        return db

    @staticmethod
    def initialize(db):
        db.execute("PRAGMA foreign_keys=ON")
        page_size = db.execute("PRAGMA page_size").fetchone()[0]
        db.execute(f"PRAGMA max_page_count={128 * 1024 * 1024 // page_size}")
        db.executescript("""
            CREATE TABLE IF NOT EXISTS campaigns(
                id TEXT PRIMARY KEY, account TEXT, updated REAL, closed INTEGER, data TEXT);
            CREATE TABLE IF NOT EXISTS links(
                account TEXT, deal TEXT, campaign TEXT REFERENCES campaigns(id) ON DELETE CASCADE,
                PRIMARY KEY(account,deal));
            CREATE TABLE IF NOT EXISTS events(
                id TEXT PRIMARY KEY, campaign TEXT REFERENCES campaigns(id) ON DELETE CASCADE,
                at REAL, kind TEXT, payload TEXT);
            CREATE TABLE IF NOT EXISTS samples(
                id INTEGER PRIMARY KEY, campaign TEXT REFERENCES campaigns(id) ON DELETE CASCADE,
                at REAL, payload TEXT);
            CREATE TABLE IF NOT EXISTS path(
                id TEXT PRIMARY KEY, campaign TEXT REFERENCES campaigns(id) ON DELETE CASCADE,
                at REAL, payload TEXT);
            CREATE INDEX IF NOT EXISTS event_campaign ON events(campaign,at);
            CREATE INDEX IF NOT EXISTS sample_campaign ON samples(campaign,at);
            CREATE INDEX IF NOT EXISTS path_campaign ON path(campaign,at);
        """)

    def prune(self, db, now):
        for table, cap in (("samples", self.sample_cap), ("events", self.event_cap)):
            db.execute(f"DELETE FROM {table} WHERE at < ?", (now - self.retention,))
            db.execute(f"DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table} ORDER BY at DESC,rowid DESC LIMIT -1 OFFSET ?)", (cap,))
        db.execute("DELETE FROM campaigns WHERE closed=1 AND updated < ?", (now - self.retention,))
        db.execute("DELETE FROM campaigns WHERE id IN (SELECT id FROM campaigns WHERE closed=1 ORDER BY updated DESC LIMIT -1 OFFSET ?)", (self.campaign_cap,))

    def observe(self, state, signals, *, account, now, unit, event="sample", position=None, details=None, cadence=None):
        # Materialize a private whitelisted snapshot, never retain live dict references.
        legs = [p for p in [state.get("open_position"), *state.get("pyramid_legs", [])] if p]
        if position and not any(p.get("deal_id") == position.get("deal_id") for p in legs):
            legs.append(position)
        opening_account = ((legs[0].get("broker_entry_evidence") or {}).get("account_id")
                           if legs else None)
        account_basis = "retained_opening_account_evidence" if opening_account else "current_session_only"
        account = opening_account or account
        legs = json.loads(json.dumps([{k: p[k] for k in FIELDS if k in p} for p in legs]))
        account_key = account or "account_unavailable"
        details = dict(details or {})
        db = self.connect()
        try:
            with db:
                if event == "global_mode_transition":
                    # Account events share the existing bounded event store.
                    # NULL campaign is intentional: transitions can occur flat.
                    payload = dict(details)
                    payload.update(account=account_key, observed_at=now)
                    self.event(db, None, now, event, payload,
                               json.dumps([account_key, details], sort_keys=True))
                    self.prune(db, now)
                    return
                if not legs:
                    if (state.get("pyramid_entry_pending") or state.get("recovery_unresolved_positions")
                            or state.get("orphan_suspected") or state.get("manual_review_required")):
                        for (cid,) in db.execute("SELECT id FROM campaigns WHERE account=? AND closed=0", (account_key,)).fetchall():
                            self.event(db, cid, now, "tracking_empty_outcome_unresolved",
                                       {"observed_at": now, "finality": "UNKNOWN"}, "unresolved")
                        self.prune(db, now)
                        return
                    # Local management flatness, explicitly not broker-certified finality.
                    for cid, raw in db.execute("SELECT id,data FROM campaigns WHERE account=? AND closed=0", (account_key,)).fetchall():
                        data = json.loads(raw)
                        if set(data.get("last_open_deals", [])) - set(data.get("closed_deals", [])):
                            data["realized_incomplete"] = True
                        data["closed_at"] = now
                        data["closure_basis"] = "June_tracking_flat_not_broker_finality"
                        self.event(db, cid, now, "final_campaign_close", data, "final")
                        db.execute("UPDATE campaigns SET closed=1,updated=?,data=? WHERE id=?", (now, json.dumps(data), cid))
                    self.prune(db, now)
                    return
                primary = legs[0]
                identity = primary.get("deal_id") or primary.get("deal_ref")
                if not identity:
                    raise ValueError("telemetry identity unavailable; no fabricated broker identity")
                found = db.execute("SELECT campaign FROM links WHERE account=? AND deal=?", (account_key, identity)).fetchone()
                cid = found[0] if found else hashlib.sha256(json.dumps([account_key, identity]).encode()).hexdigest()
                if event == "after_evaluation" and not state.get("pyramid_entry_pending"):
                    # A close and re-entry can occur in one evaluation, so there
                    # may never be a flat callback between the two campaigns.
                    for old_id, old_raw in db.execute("SELECT id,data FROM campaigns WHERE account=? AND closed=0 AND id!=?", (account_key, cid)).fetchall():
                        old_data = json.loads(old_raw)
                        current_ids = {leg.get("deal_id") or leg.get("deal_ref") for leg in legs}
                        if current_ids.intersection(old_data.get("last_open_deals", [])):
                            continue
                        old_data["closed_at"] = now
                        old_data["closure_basis"] = "June_tracking_replaced_not_broker_finality"
                        if set(old_data.get("last_open_deals", [])) - set(old_data.get("closed_deals", [])):
                            old_data["realized_incomplete"] = True
                        self.event(db, old_id, now, "final_campaign_close", old_data, "final")
                        db.execute("UPDATE campaigns SET closed=1,updated=?,data=? WHERE id=?", (now, json.dumps(old_data), old_id))
                row = db.execute("SELECT data FROM campaigns WHERE id=?", (cid,)).fetchone()
                data = json.loads(row[0]) if row else dict(
                    account=account, campaign_id=cid, primary_deal_id=primary.get("deal_id"),
                    account_basis=account_basis,
                    instrument=primary.get("instrument"), direction=primary.get("direction"),
                    identity_basis="broker_deal" if primary.get("deal_id") and account else "incomplete_identity",
                    started_observing_at=now, extrema={}, realized_by_deal={}, once=[],
                    resolution="normal_live_evaluation_samples_not_tick_perfect", cadence_seconds=cadence)
                db.execute("INSERT OR IGNORE INTO campaigns VALUES(?,?,?,?,?)", (cid, account_key, now, 0, json.dumps(data)))
                for leg in legs:
                    key = leg.get("deal_id") or leg.get("deal_ref")
                    if key:
                        db.execute("INSERT OR IGNORE INTO links VALUES(?,?,?)", (account_key, key, cid))
                    partial = number(leg.get("partial_dollar_pnl"))
                    if partial is not None and key not in data.get("closed_deals", []):
                        data["realized_by_deal"][key] = partial
                event_leg = (position or primary).get("deal_id") or (position or primary).get("deal_ref")
                if event == "leg_closed":
                    pnl = number(details.get("realized_pnl"))
                    if pnl is None and position and number(details.get("exit_price")) is not None:
                        try:
                            sign = 1 if position["direction"] == "long" else -1
                            pnl = (sign * (details["exit_price"] - position["fill_price"]) /
                                   position["fill_price"] * position["ig_size"] *
                                   unit(position["instrument"], position["fill_price"]))
                        except (KeyError, ValueError, TypeError, ZeroDivisionError):
                            pass
                    if pnl is not None:
                        data["realized_by_deal"][event_leg] = pnl
                    else:
                        data["realized_incomplete"] = True
                    closed = data.setdefault("closed_deals", [])
                    if event_leg not in closed:
                        closed.append(event_leg)
                    self.event(db, cid, now, "addon_closed" if (position or {}).get("leg_index") else "primary_closed", details, event_leg)
                observed_ids = {leg.get("deal_id") or leg.get("deal_ref") for leg in legs}
                if set(data.get("last_open_deals", [])) - observed_ids - set(data.get("closed_deals", [])):
                    data["realized_incomplete"] = True
                    self.event(db, cid, now, "unpriced_leg_disappearance", {"observed_ids": sorted(observed_ids)}, str(now))
                data["last_open_deals"] = sorted(observed_ids - set(data.get("closed_deals", [])))
                realized = sum(data["realized_by_deal"].values())
                economics, exposure, current_exposure, quantity, unrealized, stop_pnl = [], 0., 0., 0., 0., 0.
                valid = True
                exposure_known = True
                for leg in legs:
                    if (leg.get("deal_id") or leg.get("deal_ref")) in data.get("closed_deals", []):
                        continue
                    sig = signals.get(leg.get("instrument"), {})
                    px, source = executable(sig, leg["direction"])
                    fill, qty = number(leg.get("fill_price")), number(leg.get("ig_size"))
                    item = dict(leg, executable_price=px, price_basis=source,
                                quote_timestamp=sig.get("timestamp") or sig.get("price_timestamp"),
                                bid=sig.get("bid"), offer=sig.get("offer"), mid=sig.get("price"), spread_pct=sig.get("spread_pct"))
                    try:
                        if not fill or fill <= 0 or not qty or qty <= 0:
                            raise ValueError("quantity/fill unavailable")
                        quantity += qty
                        entry_n = qty * unit(leg["instrument"], fill)
                        stop = effective(leg, state.get("pyramid_agg_stop_level"))
                        sign = 1 if leg["direction"] == "long" else -1
                        item.update(actual_entry_notional=entry_n, strongest_effective_stop=stop,
                                    estimated_pnl_at_stop=sign * (stop - fill) / fill * entry_n if stop else None)
                        exposure += entry_n
                        if px is None or stop is None:
                            raise ValueError("executable price/protection missing")
                        item["current_exposure"] = qty * unit(leg["instrument"], px)
                        item["open_pnl"] = sign * (px - fill) / fill * entry_n
                        item["return"] = sign * (px - fill) / fill
                        current_exposure += item["current_exposure"]
                        unrealized += item["open_pnl"]
                        stop_pnl += item["estimated_pnl_at_stop"]
                    except (ValueError, TypeError, KeyError, ZeroDivisionError):
                        valid = False
                        if "actual_entry_notional" not in item:
                            exposure_known = False
                        item["economics_gap"] = True
                    economics.append(item)
                if "return_denominator" not in data and economics:
                    data["return_denominator"] = number(primary.get("original_notional")) or economics[0].get("actual_entry_notional")
                    data["return_basis"] = "original_primary_entry_notional_or_first_observed_basis"
                total_pnl = realized + unrealized if valid else None
                denom = data.get("return_denominator")
                ret = total_pnl / denom if total_pnl is not None and denom else None
                payload = dict(account=account, campaign_id=cid, primary_deal_id=data["primary_deal_id"],
                               instrument=data["instrument"], direction=data["direction"], observed_at=now,
                               legs=economics, quantity=quantity, entry_exposure=exposure if exposure_known else None,
                               current_exposure=current_exposure if valid else None,
                               realized_pnl_known_to_june=realized, estimated_campaign_pnl=total_pnl,
                               estimated_pnl_at_protection=realized + stop_pnl if valid else None,
                               costs_status="UNKNOWN" if not valid or data.get("realized_incomplete") else "PROVISIONAL",
                               economics_basis="June_estimates_not_broker_certified_net",
                               elapsed_since_sample=now-data["last_sample_at"] if "last_sample_at" in data else None,
                               return_value=ret, event_details=details)
                data["maximum_campaign_exposure"] = max(data.get("maximum_campaign_exposure", 0.), current_exposure, exposure)
                if event == "sample":
                    data["last_sample_at"] = now
                    db.execute("INSERT INTO samples(campaign,at,payload) VALUES(?,?,?)", (cid, now, json.dumps(payload)))
                    if valid and economics:
                        self.extreme(data, "mae_dollars", total_pnl, now, minimum=True)
                        self.extreme(data, "mfe_dollars", total_pnl, now)
                        if ret is not None:
                            self.extreme(data, "mae_return", ret, now, minimum=True)
                            self.extreme(data, "mfe_return", ret, now)
                        original_primary = next((leg for leg in economics if leg.get("deal_id") == data["primary_deal_id"]), None)
                        if original_primary:
                            self.extreme(data, "primary_peak_return", original_primary["return"], now)
                            self.extreme(data, "primary_peak_dollars", original_primary["open_pnl"], now)
                        self.extreme(data, "campaign_peak_dollars", total_pnl, now)
                        if ret is not None:
                            self.extreme(data, "campaign_peak_return", ret, now)
                        p_ret = economics[0]["return"]
                        tp = number(primary.get("tp_pct"))
                        for kind, reached in (("first_favorable_movement", p_ret > 0),
                                              ("pyramid_threshold_first_reached", p_ret >= details.get("pyramid_trigger", math.inf)),
                                              ("tp_reached", tp is not None and p_ret >= tp)):
                            if reached and kind not in data["once"]:
                                self.event(db, cid, now, kind, payload, kind)
                                data["once"].append(kind)
                    else:
                        self.event(db, cid, now, "observation_gap", payload, str(now))
                elif event != "after_evaluation":
                    # Repeated sync/partial requests deduplicate by meaningful state.
                    token = json.dumps([event_leg, details.get("attempt"), details.get("reason"),
                                        [(l.get("ig_size"), l.get("dple_effective_sl"), l.get("intended_stop_level")) for l in legs]], sort_keys=True)
                    if event in ("entry", "addon_opened", "addon_accepted", "dple_m1", "dple_m2", "mpd_activation"):
                        token = event_leg
                    self.event(db, cid, now, event, payload, token)
                    if event == "exit_requested":
                        reason = details.get("reason", "")
                        kind = ("max_hold_exit" if reason == "max_hold" else "reversal_exit" if reason == "reversal"
                                else "stop_exit" if reason in ("stop_loss", "dple_trail", "mpd_floor", "pyramid_agg_stop", "pyramid_leg_sl_close_primary") else None)
                        if kind:
                            self.event(db, cid, now, kind, payload, event_leg)
                protection = json.dumps([(l.get("deal_id"), l.get("ig_size"), l.get("intended_stop_level"),
                                          l.get("broker_stop_level"), l.get("defensive_soft_sl"), l.get("dple_effective_sl")) for l in legs])
                if data.get("protection_fingerprint") != protection:
                    self.event(db, cid, now, "protection_economics", payload, protection)
                    data["protection_fingerprint"] = protection
                data["last_observation"] = payload
                if not data["last_open_deals"] and not state.get("pyramid_entry_pending"):
                    data["closed_at"] = now
                    data["closure_basis"] = "June_observed_leg_closures_not_broker_net_finality"
                    self.event(db, cid, now, "final_campaign_close", data, "final")
                    db.execute("UPDATE campaigns SET closed=1 WHERE id=?", (cid,))
                db.execute("UPDATE campaigns SET updated=?,data=? WHERE id=?", (now, json.dumps(data), cid))
                self.prune(db, now)
        finally:
            db.close()

    # ── Durable per-campaign price-path recorder (winner-starvation-d78de77) ──
    # Observation-only. Writes ONE bounded, versioned, deduplicated row per open
    # campaign per successful poll into a dedicated `path` table. Never calls the
    # broker, never mutates trading state, never returns a decision. Any failure is
    # swallowed by the caller (_live_record_path). Retention/caps mirror samples.
    PATH_SCHEMA_VERSION = 1

    def record_path(self, state, signals, *, account, now, unit, context=None):
        """Append a durable per-campaign path row.

        `context` carries poll-time quantities the ordinary sample lacks:
        ATR/fallback, signal/conviction, allocation consumed/remaining, F50
        economics, pyramid trigger status, and the last pyramid decision/reason.
        All values are June estimates, never broker-certified. Idempotent within a
        poll: the row id hashes (campaign, bucketed timestamp, leg/quantity/price
        fingerprint) so a repeated call in the same poll cannot duplicate.
        """
        legs = [p for p in [state.get("open_position"), *state.get("pyramid_legs", [])] if p]
        if not legs:
            return
        primary = legs[0]
        identity = primary.get("deal_id") or primary.get("deal_ref")
        if not identity:
            return
        context = dict(context or {})
        account_key = account or "account_unavailable"
        inst = primary.get("instrument")
        dirn = primary.get("direction")
        sig = (signals or {}).get(inst, {}) if signals else {}
        px, source = executable(sig, dirn) if dirn in ("long", "short") else (None, "missing")
        leg_rows, primary_qty, addon_qty, unrealized = [], 0.0, 0.0, 0.0
        for leg in legs:
            fill, qty = number(leg.get("fill_price")), number(leg.get("ig_size"))
            sign = 1 if leg.get("direction") == "long" else -1
            open_pnl = None
            if fill and qty and px is not None and fill > 0:
                try:
                    open_pnl = sign * (px - fill) / fill * qty * unit(leg["instrument"], fill)
                    unrealized += open_pnl
                except Exception:
                    open_pnl = None
            if leg.get("leg_index"):
                addon_qty += qty or 0.0
            else:
                primary_qty += qty or 0.0
            leg_rows.append(dict(
                deal_id=leg.get("deal_id"), leg_index=leg.get("leg_index"),
                fill_price=fill, ig_size=qty, open_pnl=open_pnl,
                intended_stop_level=leg.get("intended_stop_level"),
                acknowledged_stop_level=leg.get("acknowledged_stop_level"),
                broker_stop_level=leg.get("broker_stop_level"),
                defensive_soft_sl=leg.get("defensive_soft_sl"),
                dple_effective_sl=leg.get("dple_effective_sl"),
                stop_sync_status=(leg.get("stop_sync") or {}).get("status")))
        realized = number(context.get("realized_pnl_known_to_june"))
        payload = dict(
            schema=self.PATH_SCHEMA_VERSION, account=account_key, observed_at=now,
            campaign_hint=identity, instrument=inst, direction=dirn,
            executable_bid=number(sig.get("bid")), executable_offer=number(sig.get("offer")),
            mid=number(sig.get("price")), executable_price=px, executable_basis=source,
            spread_pct=number(sig.get("spread_pct")),
            quote_timestamp=sig.get("timestamp") or sig.get("price_timestamp"),
            primary_fill=number(primary.get("fill_price")),
            primary_quantity=primary_qty, addon_quantity=addon_qty, legs=leg_rows,
            unrealized_local_pnl=unrealized, realized_pnl_known_to_june=realized,
            # context fields (poll-time, estimate-only):
            atr_5m=number(context.get("atr_5m")), atr_fallback=context.get("atr_fallback"),
            signal_conviction=context.get("conviction"), signal_direction=sig.get("direction"),
            global_mode=state.get("global_mode", "normal"),
            instrument_mode=(state.get("instrument_mode") or {}).get(inst, "normal"),
            allocation_consumed=number(context.get("allocation_consumed")),
            allocation_remaining=number(context.get("allocation_remaining")),
            f50_protected_before=number(context.get("f50_protected_before")),
            f50_legal_ig=number(context.get("f50_legal_ig")),
            pyramid_trigger_pct=number(context.get("pyramid_trigger_pct")),
            pyramid_threshold_reached=context.get("pyramid_threshold_reached"),
            pyramid_decision=context.get("pyramid_decision"),
            pyramid_reason=context.get("pyramid_reason"),
            protection_fingerprint_present=bool(context),
            costs_status="PROVISIONAL", economics_basis="June_estimates_not_broker_certified_net")
        # Idempotency: bucket timestamp to the poll cadence and fingerprint the
        # path-defining fields, so a duplicate call in the same poll is ignored but
        # genuine subsequent polls (new price/quantity/stop state) are retained.
        fingerprint = json.dumps([
            identity, round(now, 0),
            [(r["deal_id"], r["ig_size"], r["fill_price"], r["acknowledged_stop_level"],
              r["stop_sync_status"]) for r in leg_rows],
            payload["mid"], payload["executable_price"]], sort_keys=True)
        row_id = hashlib.sha256(fingerprint.encode()).hexdigest()
        db = self.connect()
        try:
            with db:
                found = db.execute("SELECT campaign FROM links WHERE account=? AND deal=?",
                                   (account_key, identity)).fetchone()
                cid = found[0] if found else hashlib.sha256(
                    json.dumps([account_key, identity]).encode()).hexdigest()
                # The campaign row is created by the ordinary sample observe that runs
                # earlier in the same poll. If it is somehow absent, link the path row
                # to a NULL campaign rather than violating the FK — the row is still
                # usable (it carries campaign_hint) and nothing crashes.
                exists = db.execute("SELECT 1 FROM campaigns WHERE id=?", (cid,)).fetchone()
                db.execute("INSERT OR IGNORE INTO path(id,campaign,at,payload) VALUES(?,?,?,?)",
                           (row_id, cid if exists else None, now, json.dumps(payload)))
                self.prune_path(db, now)
        finally:
            db.close()

    def prune_path(self, db, now):
        db.execute("DELETE FROM path WHERE at < ?", (now - self.retention,))
        db.execute(
            "DELETE FROM path WHERE rowid IN (SELECT rowid FROM path ORDER BY at DESC,rowid DESC LIMIT -1 OFFSET ?)",
            (self.sample_cap,))

    @staticmethod
    def event(db, cid, now, kind, payload, token):
        key = hashlib.sha256(json.dumps([cid, kind, token], sort_keys=True).encode()).hexdigest()
        db.execute("INSERT OR IGNORE INTO events VALUES(?,?,?,?,?)", (key, cid, now, kind, json.dumps(payload)))

    @staticmethod
    def extreme(data, name, value, now, minimum=False):
        old = data["extrema"].get(name)
        if old is None or (value < old["value"] if minimum else value > old["value"]):
            data["extrema"][name] = dict(value=value, at=now)


def default_store():
    return Store(Path(__file__).with_name("campaign_telemetry.sqlite3"))
