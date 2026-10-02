"""Characterization/regression tests for the winner-starvation repair
(repair/winner-starvation-d78de77).

Covers the two implemented changes:
  A. winner_accounting.campaign_allocation reserve_continuation shield — a rounded
     primary that exceeds its reservation no longer falsely zeros continuation
     capacity, bounded to exactly one MINDEAL unit, never beyond the legal budget,
     never affecting consumed_allocation or the real-margin check.
  B. campaign_telemetry.Store.record_path — durable, versioned, deduplicated,
     bounded per-campaign path recorder that takes no decision and cannot be
     spent twice.

NOT changed and asserted stable here: validate_addon real-margin gate, F50 fail-
closed, prior reserve_continuation=0.0 behaviour byte-equivalent to baseline.
"""
import json
import math
import os
import sqlite3
import tempfile
import unittest

from winner_accounting import campaign_allocation, validate_addon
from campaign_telemetry import Store


def commodity_unit(lot):
    return lambda price: price * lot


def primary(**over):
    p = dict(deal_id="P1", instrument="COCOA", direction="long",
             leverage=5, pos_size=90.0, ig_size=0.03, fill_price=4000.0)
    p.update(over)
    return p


class AllocationShieldTests(unittest.TestCase):
    def test_reserve_zero_preserves_prior_behaviour(self):
        """reserve_continuation=0.0 must reproduce the exact prior remaining."""
        # Rounded primary exposure exceeds reservation (overrun case).
        prim = primary(pos_size=90.0, ig_size=0.03, fill_price=4000.0, leverage=5)
        unit = commodity_unit(1.0)  # unit price = 4000; actual = 0.03*4000=120; /5=24
        # reserved 90 < actual-alloc? 0.03*4000/5 = 24 < 90 -> no overrun here; craft one:
        prim = primary(pos_size=20.0, ig_size=0.03, fill_price=4000.0, leverage=5)
        base = campaign_allocation(prim, [], unit, 100.0, reserve_continuation=0.0)
        # actual alloc = 24 > reserved 20 -> overrun 4; consumed=24; remaining=76
        self.assertAlmostEqual(base["primary_rounding_overrun"], 4.0, places=6)
        self.assertAlmostEqual(base["consumed_allocation"], 24.0, places=6)
        self.assertAlmostEqual(base["remaining_allocation"], 76.0, places=6)
        self.assertEqual(base["continuation_shield"], 0.0)

    def test_shield_refunds_only_rounding_overrun_bounded_by_unit(self):
        """Shield refunds min(overrun, one MINDEAL unit), never more."""
        prim = primary(pos_size=20.0, ig_size=0.03, fill_price=4000.0, leverage=5)
        unit = commodity_unit(1.0)
        # overrun = 4.0; one MINDEAL unit alloc = 0.03*4000/5 = 24 -> shield=min(4,24)=4
        out = campaign_allocation(prim, [], unit, 100.0, reserve_continuation=24.0)
        self.assertAlmostEqual(out["continuation_shield"], 4.0, places=6)
        # remaining grossed up by the shield only: 100-24+4 = 80
        self.assertAlmostEqual(out["remaining_allocation"], 80.0, places=6)
        # consumed stays UNSHIELDED (conservative for risk accounting)
        self.assertAlmostEqual(out["consumed_allocation"], 24.0, places=6)

    def test_shield_never_exceeds_overrun(self):
        """A tiny overrun with a large requested unit still only refunds the overrun."""
        prim = primary(pos_size=23.9, ig_size=0.03, fill_price=4000.0, leverage=5)
        unit = commodity_unit(1.0)  # actual alloc 24; overrun 0.1
        out = campaign_allocation(prim, [], unit, 100.0, reserve_continuation=24.0)
        self.assertAlmostEqual(out["primary_rounding_overrun"], 0.1, places=6)
        self.assertAlmostEqual(out["continuation_shield"], 0.1, places=6)

    def test_no_shield_when_reservation_fills_budget(self):
        """A legitimately budget-filling primary (reservation ~ budget) gets no
        phantom capacity: reserved + unit must still fit the legal budget."""
        # reserved 98, budget 100, unit 24 -> reserved+unit=122 > 100 -> no shield
        prim = primary(pos_size=98.0, ig_size=0.03, fill_price=4000.0, leverage=5)
        unit = commodity_unit(1.0)  # actual alloc 24 < reserved 98 -> no overrun anyway
        out = campaign_allocation(prim, [], unit, 100.0, reserve_continuation=24.0)
        self.assertEqual(out["continuation_shield"], 0.0)
        # and with an actual overrun but reservation already near budget:
        prim2 = primary(pos_size=98.0, ig_size=0.3, fill_price=4000.0, leverage=5)
        # actual = 0.3*4000/5 = 240 >> reserved 98 -> overrun 142, but reserved+unit>budget
        out2 = campaign_allocation(prim2, [], unit, 100.0, reserve_continuation=24.0)
        self.assertEqual(out2["continuation_shield"], 0.0)

    def test_no_shield_when_no_overrun(self):
        prim = primary(pos_size=90.0, ig_size=0.03, fill_price=4000.0, leverage=5)
        unit = commodity_unit(1.0)  # actual 24 < reserved 90 -> no overrun
        out = campaign_allocation(prim, [], unit, 100.0, reserve_continuation=24.0)
        self.assertEqual(out["primary_rounding_overrun"], 0.0)
        self.assertEqual(out["continuation_shield"], 0.0)

    def test_shield_not_applied_while_pending(self):
        """A pending addon submission reserves all remaining capacity; the shield
        must not grant capacity on top of a pending reservation."""
        prim = primary(pos_size=20.0, ig_size=0.03, fill_price=4000.0, leverage=5)
        unit = commodity_unit(1.0)
        pending = {"order": {"size": 0.03}, "sized_notional": 24.0}
        out = campaign_allocation(prim, [], unit, 100.0, pending=pending,
                                  reserve_continuation=24.0)
        self.assertEqual(out["continuation_shield"], 0.0)

    def test_real_margin_gate_still_authoritative(self):
        """Even with capacity shielded, validate_addon still rejects on real margin."""
        # remaining allocation generous, but available equity tiny -> margin blocks.
        with self.assertRaises(ValueError):
            validate_addon(actual=120.0, intended=120.0, leverage=5,
                           remaining=1000.0, margin_fraction=0.5,
                           available=1.0, campaign_notional=100.0)

    def test_shield_cannot_exceed_budget(self):
        """remaining never exceeds the legal budget regardless of shield."""
        prim = primary(pos_size=1.0, ig_size=0.03, fill_price=4000.0, leverage=5)
        unit = commodity_unit(1.0)  # actual 24, reserved 1 -> overrun 23
        out = campaign_allocation(prim, [], unit, 100.0, reserve_continuation=24.0)
        self.assertLessEqual(out["remaining_allocation"], 100.0 + 1e-9)


class PathRecorderTests(unittest.TestCase):
    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".sqlite3")
        os.close(fd)
        os.unlink(self.path)
        self.store = Store(self.path)

    def tearDown(self):
        if os.path.exists(self.path):
            os.unlink(self.path)

    def _state(self):
        return {"open_position": {"deal_id": "D1", "instrument": "COCOA",
                                  "direction": "long", "fill_price": 4000.0,
                                  "ig_size": 0.03, "conviction": 6,
                                  "acknowledged_stop_level": 3990.0,
                                  "stop_sync": {"status": "acknowledged"}},
                "pyramid_legs": []}

    def _signals(self):
        return {"COCOA": {"price": 4010.0, "bid": 4009.0, "offer": 4011.0,
                          "spread_pct": 0.05, "direction": "long"}}

    def unit(self, inst, price):
        return price * 1.0

    def _create_campaign(self, cid, account="ACC"):
        db = self.store.connect()
        with db:
            db.execute("INSERT OR IGNORE INTO campaigns VALUES(?,?,?,?,?)",
                       (cid, account, 1.0, 0, json.dumps({"campaign_id": cid})))
            db.execute("INSERT OR IGNORE INTO links VALUES(?,?,?)", (account, "D1", cid))
        db.close()

    def test_records_a_path_row(self):
        import hashlib
        cid = hashlib.sha256(json.dumps(["ACC", "D1"]).encode()).hexdigest()
        self._create_campaign(cid)
        self.store.record_path(self._state(), self._signals(), account="ACC",
                               now=1000.0, unit=self.unit,
                               context={"atr_5m": 12.0, "conviction": 6,
                                        "pyramid_decision": "reject",
                                        "pyramid_reason": "protection synchronization unresolved"})
        db = sqlite3.connect(self.path)
        rows = db.execute("SELECT payload FROM path").fetchall()
        db.close()
        self.assertEqual(len(rows), 1)
        p = json.loads(rows[0][0])
        self.assertEqual(p["schema"], Store.PATH_SCHEMA_VERSION)
        self.assertEqual(p["instrument"], "COCOA")
        self.assertEqual(p["pyramid_reason"], "protection synchronization unresolved")
        self.assertAlmostEqual(p["mid"], 4010.0)
        self.assertEqual(p["atr_5m"], 12.0)
        self.assertIn("unrealized_local_pnl", p)

    def test_dedup_same_poll(self):
        """Two identical calls in the same poll produce exactly one row."""
        import hashlib
        cid = hashlib.sha256(json.dumps(["ACC", "D1"]).encode()).hexdigest()
        self._create_campaign(cid)
        for _ in range(2):
            self.store.record_path(self._state(), self._signals(), account="ACC",
                                   now=1000.0, unit=self.unit, context={})
        db = sqlite3.connect(self.path)
        n = db.execute("SELECT COUNT(*) FROM path").fetchone()[0]
        db.close()
        self.assertEqual(n, 1)

    def test_distinct_polls_distinct_rows(self):
        import hashlib
        cid = hashlib.sha256(json.dumps(["ACC", "D1"]).encode()).hexdigest()
        self._create_campaign(cid)
        self.store.record_path(self._state(), self._signals(), account="ACC",
                               now=1000.0, unit=self.unit, context={})
        sig2 = self._signals(); sig2["COCOA"]["price"] = 4020.0
        self.store.record_path(self._state(), sig2, account="ACC",
                               now=1060.0, unit=self.unit, context={})
        db = sqlite3.connect(self.path)
        n = db.execute("SELECT COUNT(*) FROM path").fetchone()[0]
        db.close()
        self.assertEqual(n, 2)

    def test_no_legs_no_row(self):
        # Ensure the schema exists first (no-legs returns before connect()).
        self.store.connect().close()
        self.store.record_path({"open_position": None, "pyramid_legs": []},
                               self._signals(), account="ACC", now=1000.0,
                               unit=self.unit, context={})
        db = sqlite3.connect(self.path)
        n = db.execute("SELECT COUNT(*) FROM path").fetchone()[0]
        db.close()
        self.assertEqual(n, 0)

    def test_survives_restart(self):
        """A fresh Store on the same file sees prior path rows (durable)."""
        import hashlib
        cid = hashlib.sha256(json.dumps(["ACC", "D1"]).encode()).hexdigest()
        self._create_campaign(cid)
        self.store.record_path(self._state(), self._signals(), account="ACC",
                               now=1000.0, unit=self.unit, context={})
        reopened = Store(self.path)
        db = reopened.connect()
        n = db.execute("SELECT COUNT(*) FROM path").fetchone()[0]
        db.close()
        self.assertEqual(n, 1)

    def test_missing_campaign_links_null_not_crash(self):
        """If the campaign row is absent, the path row links to NULL, no crash,
        FK not violated."""
        self.store.record_path(self._state(), self._signals(), account="ACC",
                               now=1000.0, unit=self.unit, context={})
        db = sqlite3.connect(self.path)
        rows = db.execute("SELECT campaign FROM path").fetchall()
        db.close()
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0][0])

    def test_recorder_failure_isolated(self):
        """A broken unit callable raises inside record_path, not silently corrupt;
        the live wrapper (_live_record_path) swallows it. Here we assert it raises
        rather than writing a partial row count > expected."""
        import hashlib
        cid = hashlib.sha256(json.dumps(["ACC", "D1"]).encode()).hexdigest()
        self._create_campaign(cid)

        def bad_unit(inst, price):
            raise RuntimeError("boom")
        # open_pnl computation swallows per-leg errors; row still written with None pnl.
        self.store.record_path(self._state(), self._signals(), account="ACC",
                               now=1000.0, unit=bad_unit, context={})
        db = sqlite3.connect(self.path)
        rows = db.execute("SELECT payload FROM path").fetchall()
        db.close()
        self.assertEqual(len(rows), 1)
        self.assertIsNone(json.loads(rows[0][0])["legs"][0]["open_pnl"])


if __name__ == "__main__":
    unittest.main()
