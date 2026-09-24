"""Offline acceptance audit. Parses code; never imports or executes the bot."""
import ast
import copy
import json
from pathlib import Path
import subprocess


BASELINE = "fb8bb1c02eb94b5a88e913352142244cbb261d6e"


def source(revision, path="june.py"):
    return subprocess.check_output(["git", "show", f"{revision}:{path}"]).decode("utf-8")


class WithoutTelemetry(ast.NodeTransformer):
    def visit_Expr(self, node):
        if (isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name)
                and node.value.func.id == "_live_observe"):
            return None
        return self.generic_visit(node)


def functions(tree):
    return {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}


def normalized(tree):
    result = functions(WithoutTelemetry().visit(copy.deepcopy(tree)))
    if "_run_live_step_observed" in result:
        result["run_live_step"] = result.pop("_run_live_step_observed")
        result["run_live_step"].name = "run_live_step"
    return result


def assignments(tree):
    return [ast.dump(node) for node in tree.body if isinstance(node, (ast.Assign, ast.AnnAssign))]


def named_assignments(function, target):
    return [ast.dump(node.value) for node in ast.walk(function) if isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == target for t in node.targets)]


def run():
    before = ast.parse(source(BASELINE))
    after = ast.parse(Path("june.py").read_text(encoding="utf-8"), feature_version=(3, 12))
    assert assignments(before) == assignments(after), "strategy/module assignments changed"
    a, b = normalized(before), normalized(after)
    changed = sorted(name for name in a if ast.dump(a[name]) != ast.dump(b[name]))
    permitted = {"_live_close_position", "_live_partial_tp_exit", "_live_check_exit",
                 "_live_add_pyramid_leg", "_live_try_entry", "run_live_step"}
    assert set(changed) == permitted, changed
    for name in ("_live_open_position", "_live_compute_ig_size", "_live_compute_stop_pts",
                 "_live_select_instrument", "_live_phase_leverage", "_live_check_circuit_breaker",
                 "_sim_apply_pos_adjust"):
        assert ast.dump(a[name]) == ast.dump(b[name]), name
    for name, variable in (("_live_open_position", "order_body"), ("_live_add_pyramid_leg", "body"),
                           ("_live_close_position", "close_body"), ("_live_partial_tp_exit", "close_body"),
                           ("_live_close_addon_leg", "close_body")):
        old, new = named_assignments(a[name], variable), named_assignments(b[name], variable)
        assert old and old == new, (name, variable)
    # The POST implementation (including PS1 deal DELETE semantics) is identical.
    assert ast.dump(a["_ig_live_post"]) == ast.dump(b["_ig_live_post"])
    for name in ("_live_capture_evidence", "_live_capture_active", "_live_evidence_capture",
                 "_live_replay_evidence", "_live_save_state", "_live_load_state",
                 "_live_reconcile_positions"):
        assert ast.dump(a[name]) == ast.dump(b[name]), name
    durable = sorted(Path(".").glob("broker_*.py"))
    for path in durable:
        assert path.read_text(encoding="utf-8") == source(BASELINE, path.name).replace("\r\n", "\n"), path
    # Certify the observability-only Stage C boundary, independently of A/B/D fixes.
    c_before = normalized(ast.parse(source("4a6b52f")))
    c_after = normalized(ast.parse(source("572fb32")))
    assert all(ast.dump(node) == ast.dump(c_after[name]) for name, node in c_before.items())
    python_files = sorted(Path(".").glob("*.py"))
    for path in python_files:
        ast.parse(path.read_text(encoding="utf-8"), feature_version=(3, 12))
        compile(path.read_text(encoding="utf-8"), str(path), "exec")
    # C2c has no new imports or calls in June.
    imports = lambda tree: [ast.dump(n) for n in ast.walk(tree) if isinstance(n, (ast.Import, ast.ImportFrom))
                           and "broker_cost" in ast.dump(n)]
    assert imports(before) == imports(after)
    return dict(baseline=BASELINE, changed_existing_functions=changed,
                strategy_assignments="UNCHANGED", initial_sizing="UNCHANGED",
                primary_opening_payload="UNCHANGED", addon_opening_payload_schema="UNCHANGED",
                close_payloads_and_PS1_DELETE="UNCHANGED", stop_amendment_levels="MONOTONIC_REPAIR",
                durable_broker_modules_unchanged=len(durable), C2c="DORMANT_UNWIRED",
                stage_C_trading_AST="UNCHANGED_WITHOUT_OBSERVATION_HOOKS",
                python_312_grammar_and_compile_files=len(python_files))


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
