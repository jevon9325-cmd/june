"""Import provenance test — verifies june.py resolves to the current worktree.

With pytest.ini pythonpath = . in place, running:
    /opt/bots/june/venv/bin/python -m pytest (from /opt/bots/june-build3)
must import THIS directory's june.py, not any other build's.

The test is path-agnostic: it compares june.__file__ against os.getcwd()
so it remains valid if the worktree is cloned or moved.
"""
import os
import pytest


def test_june_file_is_in_current_working_directory():
    import june
    june_path = os.path.abspath(june.__file__)
    cwd = os.path.abspath(os.getcwd())
    assert june_path.startswith(cwd), (
        "june.__file__ does not resolve to the current working directory.\n"
        "Expected prefix: {}\n"
        "Got:             {}\n"
        "This means a different build's june.py is being imported."
        .format(cwd, june_path)
    )


def test_june_file_printed_for_audit():
    import june
    june_path = os.path.abspath(june.__file__)
    print("\njune.__file__ = {}".format(june_path))
    # Soft check: must end with june.py
    assert june_path.endswith("june.py"), "june.__file__ must be june.py, got: {}".format(june_path)
