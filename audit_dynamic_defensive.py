"""Offline baseline comparison: scope, constants, payloads and C2c boundaries."""
import ast
import json
from pathlib import Path
import subprocess

BASELINE = "d805ac592aac34a51d79680456dd12d1b1e3de87"


def dump(node):
    return ast.dump(node, include_attributes=False)


def functions(tree):
    return {n.name: n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}


def constants(tree):
    return {n.targets[0].id: dump(n.value) for n in tree.body if isinstance(n, ast.Assign)
            and len(n.targets) == 1 and isinstance(n.targets[0], ast.Name)
            and n.targets[0].id.isupper()}


def payloads(tree):
    names = {"_live_open_position", "_live_close_position", "_live_partial_tp_exit",
             "_live_close_addon_leg", "_live_add_pyramid_leg"}
    result = {}
    for name, fn in functions(tree).items():
        if name not in names:
            continue
        result[name] = [dump(n) for n in ast.walk(fn) if isinstance(n, ast.Dict)
                        and any(isinstance(k, ast.Constant) and k.value == "orderType" for k in n.keys)]
    return result


def main():
    old = ast.parse(subprocess.check_output(["git", "show", BASELINE + ":june.py"], text=True, encoding="utf-8"))
    new = ast.parse(Path("june.py").read_text(encoding="utf-8"))
    old_f, new_f = functions(old), functions(new)
    changed = sorted(k for k in old_f if dump(old_f[k]) != dump(new_f[k]))
    allowed = {"_live_update_defensive_mode", "_live_poll_balance", "_run_live_step_observed",
               "_live_check_pyramid_entry", "_live_add_pyramid_leg", "_live_open_position",
               "_live_close_addon_leg"}
    assert set(changed) == allowed, changed
    assert set(new_f) - set(old_f) == {"_live_defensive_scaling_evidence"}
    assert constants(old) == constants(new), "strategy constant changed"
    assert payloads(old) == payloads(new), "opening/close payload changed"
    unchanged = ("_live_check_circuit_breaker", "_live_try_entry", "_live_compute_ig_size",
                 "_live_compute_stop_pts", "_live_tier_risk_pct", "_live_check_exit",
                 "_live_partial_tp_exit", "_live_close_position", "_live_check_pyramid_exits",
                 "_live_perf_blocked", "_live_perf_record", "_live_reconcile_positions")
    for name in unchanged:
        assert dump(old_f[name]) == dump(new_f[name]), name
    for filename in ("winner_accounting.py", "winner_protection.py", "broker_reconcile.py",
                     "broker_cost.py", "broker_finality.py", "broker_pending.py"):
        baseline = subprocess.check_output(["git", "show", BASELINE + ":" + filename], text=True, encoding="utf-8")
        assert ast.dump(ast.parse(baseline)) == ast.dump(ast.parse(Path(filename).read_text(encoding="utf-8"))), filename
    calls = {name: sorted({caller for caller, fn in new_f.items()
                          if any(isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                                 and n.func.id == name for n in ast.walk(fn))})
             for name in ("_live_update_defensive_mode", "_live_check_pyramid_entry",
                          "_live_add_pyramid_leg", "_live_defensive_scaling_evidence")}
    assert calls["_live_update_defensive_mode"] == ["_run_live_step_observed"]
    assert calls["_live_add_pyramid_leg"] == ["_live_check_pyramid_entry"]
    report = dict(baseline=BASELINE, changed_functions=changed, added_functions=["_live_defensive_scaling_evidence"],
                  constants_changed=[], order_payloads_changed=False, unchanged_functions=list(unchanged),
                  callers=calls, c2c="existing modules and reconciliation unchanged; boundary tests required")
    Path("DYNAMIC_DEFENSIVE_AUDIT.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
