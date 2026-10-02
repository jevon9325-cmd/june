import importlib, pathlib, sys, pytest
root=pathlib.Path.cwd()
sys.path.insert(0,str(root))
for p in root.glob('*.py'):
    if p.name == 'run_reconciliation_regressions.py' or p.resolve() == pathlib.Path(__file__).resolve():
        continue
    if not p.name.startswith(('test_', 'audit_')) and p.stem not in ('june_etf_screener',):
        # Pin shared modules before legacy tests insert other worktrees in sys.path.
        importlib.import_module(p.stem)
raise SystemExit(pytest.main(sys.argv[1:] or ['-q']))
